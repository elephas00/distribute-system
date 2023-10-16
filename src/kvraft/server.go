package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int
	ClientId  int
	Method    string
	Key       string
	Value     string
	Err       Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate           int // snapshot if log grows this big
	logReplayOverheadBytes int
	// Your definitions here.
	stateMachine     map[string]string
	lastRequestId    map[int]int
	lastResponse     map[int]Op
	persister        *raft.Persister
	lastCommandIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ok := kv.mu.TryLock()
	if !ok {
		return
	}
	if lastRid, exist := kv.lastRequestId[args.ClientId]; exist && lastRid == args.RequestId {
		op, exist := kv.lastResponse[args.ClientId]
		kv.mu.Unlock()
		if exist {
			reply.Err = op.Err
			reply.Value = op.Value
			return
		}
	}

	getOp := Op{
		Method:    GET,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	_, _, isLeader := kv.rf.Start(getOp)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// log.Printf("leader %d receive command: %d, requestId:%d, clientId:%d\n", kv.me, index, args.RequestId, args.ClientId)
	kv.mu.Unlock()
	kv.waitGetReply(args, reply)

}

func (kv *KVServer) waitGetReply(args *GetArgs, reply *GetReply) {
	for i := 0; i < 10; i++ {
		ok := kv.mu.TryLock()
		if !ok {
			time.Sleep(time.Millisecond * 10)
			continue
		}
		rId, exist := kv.lastRequestId[args.ClientId]
		if exist && rId == args.RequestId {
			resp := kv.lastResponse[args.ClientId]
			reply.Err = resp.Err
			reply.Value = resp.Value
			// log.Printf("leader:%d finsish %+v, reply %+v\n", kv.me, args, reply)
			kv.mu.Unlock()
			return
		} else {
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}

	}
}

func (kv *KVServer) getRaftSize() int {
	return kv.logReplayOverheadBytes
}

func (kv *KVServer) applier() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			if msg.CommandIndex == kv.lastCommandIndex {
				log.Printf("warning: duplicated command %+v\n", msg)
			} else if msg.CommandIndex > kv.lastCommandIndex+1 {
				log.Printf("warning: missing some command, now %d, last %d\n", msg.CommandIndex, kv.lastCommandIndex)
			} else if msg.CommandIndex < kv.lastCommandIndex {
				log.Printf("warning: outdated command, now %d, last %d\n", msg.CommandIndex, kv.lastCommandIndex)
			}
			kv.lastCommandIndex = msg.CommandIndex
			oldStateMachineBytes := len(kv.getStateMachineBytes())
			kv.applyMsg(&msg)
			newStateMachineBytes := len(kv.getStateMachineBytes())
			kv.logReplayOverheadBytes += int(unsafe.Sizeof(msg)) + newStateMachineBytes - oldStateMachineBytes
		}
		if msg.SnapshotValid {
			kv.applySnapshot(&msg)
		}
		if kv.maxraftstate != NO_SNAPSHOT && kv.getRaftSize() > kv.maxraftstate {
			stateMachineState := kv.getStateMachineBytes()
			kv.rf.Snapshot(msg.CommandIndex, stateMachineState)
			kv.logReplayOverheadBytes = 0
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) applySnapshot(msg *raft.ApplyMsg) {
	kv.readSnapshot(msg.Snapshot)
	kv.lastCommandIndex = msg.SnapshotIndex
	kv.logReplayOverheadBytes = 0
}

func (kv *KVServer) applyMsg(msg *raft.ApplyMsg) {
	if msg.Command == nil && !msg.SnapshotValid {
		// log.Printf("server:%d apply nil command %+v\n", kv.me, msg)
		return
	}
	op, ok := msg.Command.(Op)
	if !ok {
		log.Printf("warning: command is not operation, %+v", op)
		return
	}
	// duplicate request
	if rId, exist := kv.lastRequestId[op.ClientId]; exist && rId == op.RequestId {
		return
	}
	switch op.Method {
	case APPEND:
		op.Err = OK
		if val, exist := kv.stateMachine[op.Key]; exist {
			kv.stateMachine[op.Key] = val + op.Value
		} else {
			kv.stateMachine[op.Key] = op.Value
		}
	case PUT:
		kv.stateMachine[op.Key] = op.Value
		op.Err = OK
	case GET:
		if val, exist := kv.stateMachine[op.Key]; exist {
			op.Err = OK
			op.Value = val
		} else {
			op.Err = ErrNoKey
			op.Value = ""
		}
	default:
		log.Printf("warning: unknown command %+v\n", msg)
	}

	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.lastResponse[op.ClientId] = op

	// log.Printf("server:%d apply command %d, detail %+v  \n", kv.me, msg.CommandIndex, op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ok := kv.mu.TryLock()
	if !ok {
		return
	}

	if lastRid, exist := kv.lastRequestId[args.ClientId]; exist && lastRid == args.RequestId {
		op, exist := kv.lastResponse[args.ClientId]
		kv.mu.Unlock()
		if exist {
			reply.Err = op.Err
			return
		}
	}
	putAppendOp := Op{
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if args.Op == PUT {
		putAppendOp.Method = PUT
	} else {
		putAppendOp.Method = APPEND
	}
	_, _, isLeader := kv.rf.Start(putAppendOp)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	kv.waitPutAppendReply(args, reply)
}

func (kv *KVServer) waitPutAppendReply(args *PutAppendArgs, reply *PutAppendReply) {
	for i := 0; i < 10; i++ {
		ok := kv.mu.TryLock()
		if !ok {
			time.Sleep(time.Millisecond * 10)
			continue
		}
		rId, exist := kv.lastRequestId[args.ClientId]
		// log.Printf("server %d, lastRequestId %d, args.RequestId %d \n", kv.me, rId, args.RequestId)
		if exist && rId == args.RequestId {
			// log.Printf("server %d finished request %+v, reply %+v \n", kv.me, args, reply)
			reply.Err = kv.lastResponse[args.ClientId].Err
			kv.mu.Unlock()
			return
		} else {
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}

	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	// kv.readSnapshot(persister.ReadSnapshot())

	kv.mu.Lock()
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.stateMachine = make(map[string]string)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.stateMachine = make(map[string]string)
	kv.lastRequestId = make(map[int]int)
	kv.lastResponse = make(map[int]Op)
	kv.mu.Unlock()
	go kv.applier()
	return kv
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var stateMachine map[string]string
	var lastRequestId map[int]int
	var lastResponse map[int]Op

	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastRequestId) != nil ||
		d.Decode(&lastResponse) != nil {
		log.Printf("warning: %d read persist failed", kv.me)
	} else {
		kv.stateMachine = stateMachine
		kv.lastRequestId = lastRequestId
		kv.lastResponse = lastResponse
	}
}

func (kv *KVServer) getStateMachineBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastRequestId)
	e.Encode(kv.lastResponse)
	return w.Bytes()
}

func (kv *KVServer) printStateMachine() {
	p := "leader %d state machine: [\n"
	message := fmt.Sprintf(p, kv.me)
	var builder strings.Builder
	builder.WriteString(message)
	logPattern := "{%s:%s}\n"
	for k, v := range kv.stateMachine {
		builder.WriteString(fmt.Sprintf(logPattern, k, v))
	}
	builder.WriteString("]")
	log.Println(builder.String())
}
