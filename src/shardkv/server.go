package shardkv

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"log"
	"sync"
	"time"
	"unsafe"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	CONFIG_POLLING_FIX_DELAY_MILLISECONDS = 100
	NO_SNAPSHOT                           = -1
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int
	RequestId int
	Method    string
	Shard     int
	Args      interface{}
	Reply     interface{}
}

type void struct {
}

var VOID_MEMEBER void

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	// snapshot
	logReplayOverheadBytes int
	lastCommandIndex       int
	// state machine
	stateMachine  map[string]string
	lastRequestId map[int]int
	lastResponse  map[int]Op
	persister     *raft.Persister
	// shard
	prevConfig     shardctrler.Config
	config         shardctrler.Config
	sm             *shardctrler.Clerk
	currentShards  map[int]void
	configChanging bool
}

func (kv *ShardKV) ShardsAdd(args *ShardsAddArgs, reply *ShardsAddReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.mu.RUnlock()
		reply.Err = ErrWrongLeader
		return
	}

	if kv.config.Num > args.Config.Num {
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}

	if kv.config.Num < args.Config.Num {
		if kv.config.Num+1 == args.Config.Num {
			log.Printf("group %d, server %d, num %d, shards:%+v, reject shards add request gid:%d, num:%d, shards:%+v\n", kv.gid, kv.me, kv.config.Num, kv.currentShards, args.GID, args.Config.Num, args.Shards)
			newConfig := kv.sm.Query(kv.config.Num + 1)
			kv.sendUpdateConfigLogToRaft(newConfig)
		}
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}

	if !kv.configChanging && args.Config.Num == kv.config.Num {
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}

	if lastRid, exist := kv.lastRequestId[args.ClientId]; exist && lastRid == args.RequestId {
		resp, exist := kv.lastResponse[args.ClientId].Reply.(ShardsAddReply)
		if exist {
			reply.Err = resp.Err
			kv.mu.RUnlock()
			return
		}
	}

	shardsAddOp := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Method:    MethodAddShards,
		Args:      *args,
	}

	_, _, isLeader := kv.rf.Start(shardsAddOp)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.RUnlock()
		return
	}
	// log.Printf("leader %d receive command: %d, requestId:%d, clientId:%d\n", kv.me, index, args.RequestId, args.ClientId)
	kv.mu.RUnlock()
	kv.waitShardsAddReply(args, reply)
}

func (kv *ShardKV) ShardsRemove(args *ShardsRemoveArgs, reply *ShardsRemoveReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.RUnlock()
		return
	}

	if args.Config.Num < kv.config.Num {
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}

	if args.Config.Num > kv.config.Num {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}

	if lastRid, exist := kv.lastRequestId[args.ClientId]; exist && lastRid == args.RequestId {
		resp, exist := kv.lastResponse[args.ClientId].Reply.(ShardsRemoveReply)
		if exist {
			reply.Err = resp.Err
			kv.mu.RUnlock()
			return
		}
	}

	shardsRemoveOp := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Method:    MethodRemoveShards,
		Args:      *args,
	}

	_, _, isLeader := kv.rf.Start(shardsRemoveOp)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.RUnlock()
		return
	}
	// log.Printf("leader %d receive command: %d, requestId:%d, clientId:%d\n", kv.me, index, args.RequestId, args.ClientId)
	kv.mu.RUnlock()
	kv.waitShardsRemoveReply(args, reply)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.RUnlock()
		// log.Println("WRONG LEADER")
		return
	}

	if kv.config.Num < args.ConfigNum {
		log.Printf("group %d, server %d, num %d, shards:%+v, reject get request client id:%d, request id:%d, config num: %d\n", kv.gid, kv.me, kv.config.Num, kv.currentShards, args.ClientId, args.RequestId, args.ConfigNum)
		if kv.config.Num+1 == args.ConfigNum {
			newConfig := kv.sm.Query(kv.config.Num + 1)
			kv.sendUpdateConfigLogToRaft(newConfig)
		}
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}

	if kv.config.Num > args.ConfigNum {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}

	if kv.configChanging && kv.isForbbidenOpWhenConfigChanging(args.Key) {
		kv.mu.RUnlock()
		return
	}

	if lastRid, exist := kv.lastRequestId[args.ClientId]; exist && lastRid == args.RequestId {
		resp, exist := kv.lastResponse[args.ClientId].Reply.(GetReply)
		if exist {
			reply.Err = resp.Err
			reply.Value = resp.Value
			// log.Printf("group %d, server %d, num %d, shards:%+v, already finish request, args key:%s, shard:%d, reply:%+v， args%+v \n", kv.gid, kv.me, kv.config.Num, kv.currentShards, args.Key, key2shard(args.Key), reply, args)
			kv.mu.RUnlock()
			return
		}
	}

	getOp := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Method:    MethodGet,
		Shard:     key2shard(args.Key),
		Args:      *args,
	}

	_, _, isLeader := kv.rf.Start(getOp)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.RUnlock()
		return
	}
	// log.Printf("leader %d receive command: %d, requestId:%d, clientId:%d\n", kv.me, index, args.RequestId, args.ClientId)
	kv.mu.RUnlock()
	kv.waitGetReply(args, reply)
}

func (kv *ShardKV) isValidKey(key string) bool {
	shard := key2shard(key)
	return kv.config.Shards[shard] == kv.gid
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	// log.Println("receive PUT APPEND")
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.RUnlock()
		// log.Println("WRONG LEADER")
		return
	}

	if kv.config.Num < args.ConfigNum {
		log.Printf("group %d, server %d, num %d, shards:%+v, reject put append request client id:%d, request id:%d, config num: %d\n", kv.gid, kv.me, kv.config.Num, kv.currentShards, args.ClientId, args.RequestId, args.ConfigNum)
		if kv.config.Num+1 == args.ConfigNum {
			newConfig := kv.sm.Query(kv.config.Num + 1)
			kv.sendUpdateConfigLogToRaft(newConfig)
		}
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}

	if kv.config.Num > args.ConfigNum {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}

	if kv.configChanging && kv.isForbbidenOpWhenConfigChanging(args.Key) {
		kv.mu.RUnlock()
		return
	}

	if lastRid, exist := kv.lastRequestId[args.ClientId]; exist && lastRid == args.RequestId {
		resp, exist := kv.lastResponse[args.ClientId].Reply.(PutAppendReply)
		if exist {
			reply.Err = resp.Err
			// log.Printf("group %d, server %d, num %d, shards:%+v, already finish request, args key:%s, shard:%d, reply:%+v， args%+v \n", kv.gid, kv.me, kv.config.Num, kv.currentShards, args.Key, key2shard(args.Key), reply, args)
			kv.mu.RUnlock()
			return
		}
	}
	putAppendOp := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Shard:     key2shard(args.Key),
		Args:      *args,
	}
	if args.Op == MethodPut {
		putAppendOp.Method = MethodPut
	} else {
		putAppendOp.Method = MethodAppend
	}
	_, _, isLeader := kv.rf.Start(putAppendOp)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	kv.waitPutAppendReply(args, reply)
}

func (kv *ShardKV) waitShardsAddReply(args *ShardsAddArgs, reply *ShardsAddReply) {
	for i := 0; i < 10; i++ {
		kv.mu.RLock()
		rId, exist := kv.lastRequestId[args.ClientId]
		if exist && rId == args.RequestId {
			resp := kv.lastResponse[args.ClientId].Reply.(ShardsAddReply)
			reply.Err = resp.Err
			// log.Printf("leader:%d finsish %+v, reply %+v\n", kv.me, args, reply)
			kv.mu.RUnlock()
			return
		} else {
			kv.mu.RUnlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (kv *ShardKV) waitGetReply(args *GetArgs, reply *GetReply) {
	for i := 0; i < 10; i++ {
		kv.mu.RLock()
		rId, exist := kv.lastRequestId[args.ClientId]
		if exist && rId == args.RequestId {
			resp := kv.lastResponse[args.ClientId].Reply.(GetReply)
			reply.Err = resp.Err
			reply.Value = resp.Value
			// log.Printf("leader:%d finsish %+v, reply %+v\n", kv.me, args, reply)
			kv.mu.RUnlock()
			return
		} else {
			kv.mu.RUnlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (kv *ShardKV) waitShardsRemoveReply(args *ShardsRemoveArgs, reply *ShardsRemoveReply) {
	for i := 0; i < 10; i++ {
		kv.mu.RLock()
		rId, exist := kv.lastRequestId[args.ClientId]
		if exist && rId == args.RequestId {
			resp := kv.lastResponse[args.ClientId].Reply.(ShardsRemoveReply)
			reply.Err = resp.Err
			// log.Printf("leader:%d finsish %+v, reply %+v\n", kv.me, args, reply)
			kv.mu.RUnlock()
			return
		} else {
			kv.mu.RUnlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (kv *ShardKV) waitPutAppendReply(args *PutAppendArgs, reply *PutAppendReply) {
	for i := 0; i < 10; i++ {
		kv.mu.RLock()
		rId, exist := kv.lastRequestId[args.ClientId]
		// log.Printf("server %d, lastRequestId %d, args.RequestId %d \n", kv.me, rId, args.RequestId)
		if exist && rId == args.RequestId {
			// log.Printf("server %d finished request %+v, reply %+v \n", kv.me, args, reply)
			resp := kv.lastResponse[args.ClientId].Reply.(PutAppendReply)
			reply.Err = resp.Err
			kv.mu.RUnlock()
			return
		} else {
			kv.mu.RUnlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(ShardsAddArgs{})
	labgob.Register(ShardsAddReply{})
	labgob.Register(ShardsRemoveArgs{})
	labgob.Register(ShardsRemoveReply{})
	labgob.Register(ConfigChangeArgs{})
	labgob.Register(ConfigChangeReply{})
	labgob.Register(void{})
	kv := new(ShardKV)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.stateMachine = make(map[string]string)
	kv.lastRequestId = make(map[int]int)
	kv.lastResponse = make(map[int]Op)
	kv.currentShards = make(map[int]void)
	kv.sm = shardctrler.MakeClerk(ctrlers)
	go kv.configMaintainer()
	go kv.applier()
	go kv.configChangeRoutine()
	go kv.printGroupInfo()
	kv.printStartInfo()
	return kv
}

func (kv *ShardKV) printStartInfo() {
	currentShardsArr := make([]int, len(kv.currentShards))
	for k := range kv.currentShards {
		currentShardsArr = append(currentShardsArr, k)
	}
	log.Printf("start a new server, group %d, server %d, num %d, given shards:%+v, now %+v \n", kv.gid, kv.me, kv.config.Num, kv.prevConfig.Shards, currentShardsArr)
}

func (kv *ShardKV) applier() {
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

func (kv *ShardKV) getRaftSize() int {
	return kv.logReplayOverheadBytes
}

func compressBytes(input []byte) ([]byte, error) {
	var compressedData bytes.Buffer
	writer := gzip.NewWriter(&compressedData)

	_, err := writer.Write(input)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return compressedData.Bytes(), nil
}

func decompressBytes(compressedData []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, err
	}

	decompressedData, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return decompressedData, nil
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	data, _ = decompressBytes(data)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var stateMachine map[string]string
	var lastRequestId map[int]int
	var lastResponse map[int]Op
	var currentShards map[int]void
	var configChanging bool
	var prevConfig shardctrler.Config
	var config shardctrler.Config
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastRequestId) != nil ||
		d.Decode(&lastResponse) != nil ||
		d.Decode(&currentShards) != nil ||
		d.Decode(&configChanging) != nil ||
		d.Decode(&prevConfig) != nil ||
		d.Decode(&config) != nil {
		log.Printf("warning: %d read persist failed", kv.me)
	} else {
		kv.stateMachine = stateMachine
		kv.lastRequestId = lastRequestId
		kv.lastResponse = lastResponse
		kv.currentShards = currentShards
		kv.configChanging = configChanging
		kv.prevConfig = prevConfig
		kv.config = config
	}
}

func (kv *ShardKV) applySnapshot(msg *raft.ApplyMsg) {
	kv.readSnapshot(msg.Snapshot)
	kv.lastCommandIndex = msg.SnapshotIndex
	kv.logReplayOverheadBytes = 0
}

func (kv *ShardKV) getStateMachineBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastRequestId)
	e.Encode(kv.lastResponse)
	e.Encode(kv.currentShards)
	e.Encode(kv.configChanging)
	e.Encode(kv.prevConfig)
	e.Encode(kv.config)
	originBytes := w.Bytes()
	res, _ := compressBytes(originBytes)
	return res
	// return w.Bytes()
}

func (kv *ShardKV) isForbbidenOpWhenConfigChanging(key string) bool {
	shard := key2shard(key)
	_, exist := kv.currentShards[shard]
	if kv.prevConfig.Shards[shard] == kv.gid && kv.config.Shards[shard] == kv.gid {
		return false
	}
	if kv.prevConfig.Shards[shard] != kv.gid && kv.config.Shards[shard] == kv.gid && exist {
		return false
	}
	return true
}

func (kv *ShardKV) applyMsg(msg *raft.ApplyMsg) {
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
	if rId, exist := kv.lastRequestId[op.ClientId]; exist && rId == op.RequestId && op.Method != MethodConfigChange {
		return
	}
	switch op.Method {
	case MethodAppend:
		args := op.Args.(PutAppendArgs)
		if kv.configChanging && kv.isForbbidenOpWhenConfigChanging(args.Key) {
			return
		}
		reply := PutAppendReply{}
		if kv.isValidKey(args.Key) {
			reply.Err = OK
			if val, exist := kv.stateMachine[args.Key]; exist {
				kv.stateMachine[args.Key] = val + args.Value
			} else {
				kv.stateMachine[args.Key] = args.Value
			}
		} else {
			reply.Err = ErrWrongGroup
		}
		op.Reply = reply
	case MethodPut:
		args := op.Args.(PutAppendArgs)
		if kv.configChanging && kv.isForbbidenOpWhenConfigChanging(args.Key) {
			return
		}

		reply := PutAppendReply{}
		if kv.isValidKey(args.Key) {
			kv.stateMachine[args.Key] = args.Value
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
		op.Reply = reply
	case MethodGet:
		args := op.Args.(GetArgs)
		if kv.configChanging && kv.isForbbidenOpWhenConfigChanging(args.Key) {
			return
		}

		reply := GetReply{}
		if kv.isValidKey(args.Key) {
			if val, exist := kv.stateMachine[args.Key]; exist {
				reply.Err = OK
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongGroup
		}
		op.Reply = reply
	case MethodConfigChange:
		args := op.Args.(ConfigChangeArgs)
		if args.ChangeFinish == !kv.configChanging {
			return
		}
		if args.Config.Num < kv.config.Num {
			return
		}
		if !args.ChangeFinish {
			noMove := true
			for i := 0; i < shardctrler.NShards; i++ {
				if kv.config.Shards[i] != 0 {
					noMove = false
					break
				}
			}
			if noMove {
				for i := 0; i < shardctrler.NShards; i++ {
					gid := args.Config.Shards[i]
					if gid == kv.gid {
						kv.currentShards[i] = VOID_MEMEBER
					}
				}
			}
			kv.prevConfig = kv.config
			kv.config = args.Config
			kv.configChanging = true
			log.Printf("group %d, server %d, num %d, command %d, shards:%+v config change begin, args: num:%d, shards:%+v\n", kv.gid, kv.me, kv.prevConfig.Num, msg.CommandIndex, kv.prevConfig.Shards, kv.config.Num, kv.config.Shards)

		} else {
			kv.configChanging = false
			if _, isLeader := kv.rf.GetState(); isLeader {
				log.Printf("group %d, server %d, num %d,command %d, config change finish, current shards: %+v \n", kv.gid, kv.me, kv.config.Num, msg.CommandIndex, kv.currentShards)
			}
		}
		op.Reply = ConfigChangeReply{Err: OK}
	case MethodRemoveShards:
		args := op.Args.(ShardsRemoveArgs)
		if args.Config.Num != kv.config.Num {
			if _, isLeader := kv.rf.GetState(); isLeader {
				log.Printf("group %d, server %d, receive out dated remove request: %+v", kv.gid, kv.me, args)
			}
			return
		}
		for _, shard := range args.Shards {
			delete(kv.currentShards, shard)
		}
		for k := range kv.stateMachine {
			shard := key2shard(k)
			_, exist := kv.currentShards[shard]
			if !exist {
				delete(kv.stateMachine, k)
			}
		}
		shardsMap := make([]bool, shardctrler.NShards)
		for _, shard := range args.Shards {
			shardsMap[shard] = true
		}
		for clientId, resp := range kv.lastResponse {
			switch resp.Method {
			case MethodGet, MethodPut, MethodAppend:
				if shardsMap[resp.Shard] {
					delete(kv.lastRequestId, clientId)
					delete(kv.lastResponse, clientId)
				}
			}
		}

		if kv.maxraftstate != NO_SNAPSHOT {
			stateMachineState := kv.getStateMachineBytes()
			kv.rf.Snapshot(msg.CommandIndex, stateMachineState)
			kv.logReplayOverheadBytes = 0
		}

		if _, isLeader := kv.rf.GetState(); isLeader {
			log.Printf("group %d, leader %d, num: %d, command %d, remove shards: %+v, given:%+v, now:%+v, args:%+v\n", kv.gid, kv.me, kv.config.Num, msg.CommandIndex, args.Shards, kv.getGivenShards(), kv.currentShards, args)
		}
		op.Reply = ShardsRemoveReply{Err: OK}
	case MethodAddShards:
		args := op.Args.(ShardsAddArgs)
		for _, shard := range args.Shards {
			kv.currentShards[shard] = VOID_MEMEBER
		}
		for k, v := range args.KeyValueData {
			kv.stateMachine[k] = v
		}
		for k, v := range args.LastResponse {
			rId, exist := kv.lastRequestId[k]
			if !exist || rId < v.RequestId {
				kv.lastResponse[k] = v
				kv.lastRequestId[k] = v.RequestId
			}
		}
		if _, isLeader := kv.rf.GetState(); isLeader {
			log.Printf("group %d, leader %d, num: %d, command %d, receive shards: %+v, given:%+v, now:%+v, args:%+v\n", kv.gid, kv.me, kv.config.Num, msg.CommandIndex, args.Shards, kv.getGivenShards(), kv.currentShards, args)
		}
		op.Reply = ShardsAddReply{Err: OK}
	default:
		log.Printf("warning: unknown command %+v\n", msg)
	}
	op.Args = VOID_MEMEBER
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.lastResponse[op.ClientId] = op
}

func (kv *ShardKV) configMaintainer() {
	for {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Millisecond * CONFIG_POLLING_FIX_DELAY_MILLISECONDS)
			continue
		}
		nextConfig := kv.sm.Query(LatestConfig)
		kv.mu.RLock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.RUnlock()
			time.Sleep(time.Millisecond * CONFIG_POLLING_FIX_DELAY_MILLISECONDS)
			continue
		}
		if kv.configChanging {
			//	log.Printf("group %d, server %d, waiting config change finish, num %d, current shards %+v \n", kv.gid, kv.me, kv.config.Num, kv.currentShards)
			kv.mu.RUnlock()
			time.Sleep(time.Millisecond * CONFIG_POLLING_FIX_DELAY_MILLISECONDS)
			continue
		}
		if nextConfig.Num == kv.config.Num {
			//	log.Printf("group %d, num %d, config is latest, nothing to update\n", kv.gid, kv.config.Num)
			kv.mu.RUnlock()
			time.Sleep(time.Millisecond * CONFIG_POLLING_FIX_DELAY_MILLISECONDS)
			continue
		}
		if kv.config.Num > nextConfig.Num {
			log.Printf("warning: config number %d, current config number %d\n", nextConfig.Num, kv.config.Num)
		}
		if kv.config.Num < nextConfig.Num {
			kv.sendUpdateConfigLogToRaft(nextConfig)
		}
		kv.mu.RUnlock()
		time.Sleep(time.Millisecond * CONFIG_POLLING_FIX_DELAY_MILLISECONDS)
	}
}

func (kv *ShardKV) sendUpdateConfigLogToRaft(newConfig shardctrler.Config) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	if newConfig.Num > kv.config.Num+1 {
		newConfig = kv.sm.Query(kv.config.Num + 1)
	}
	args := ConfigChangeArgs{
		ClientId:     kv.gid,
		RequestId:    newConfig.Num,
		Config:       newConfig,
		ChangeFinish: false,
	}
	configChangeOp := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Method:    MethodConfigChange,
		Args:      args,
	}
	kv.rf.Start(configChangeOp)
	// log.Printf("group %d, leader %d change config %+v\n", kv.gid, kv.me, nextConfig)
}

func (kv *ShardKV) getLeaveShards() map[int][]int {
	res := make(map[int][]int)
	for i := 0; i < shardctrler.NShards; i++ {
		gid := kv.config.Shards[i]
		_, exist := kv.currentShards[i]
		if exist && gid != kv.gid {
			shards := res[gid]
			shards = append(shards, i)
			res[gid] = shards
		}
	}
	return res
}

func (kv *ShardKV) getGivenShards() []int {
	configContainsShards := []int{}
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.config.Shards[i] == kv.gid {
			configContainsShards = append(configContainsShards, i)
		}
	}
	return configContainsShards
}

func (kv *ShardKV) shardsValid() bool {
	configContainsShards := kv.getGivenShards()
	if len(configContainsShards) != len(kv.currentShards) {
		return false
	}
	for i := 0; i < len(configContainsShards); i++ {
		shard := configContainsShards[i]
		_, exist := kv.currentShards[shard]
		if !exist {
			return false
		}
	}
	return true
}

func (kv *ShardKV) configChangeRoutine() {
	for {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Millisecond * CONFIG_POLLING_FIX_DELAY_MILLISECONDS)
			continue
		}

		kv.mu.RLock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.RUnlock()
			time.Sleep(time.Millisecond * CONFIG_POLLING_FIX_DELAY_MILLISECONDS)
			continue
		}

		if !kv.configChanging {
			kv.mu.RUnlock()
			time.Sleep(time.Millisecond * CONFIG_POLLING_FIX_DELAY_MILLISECONDS)
			continue
		}
		// log.Printf("group:%d, server:%d, num:%d, config change routine waiting, given shards:%+v, now %+v\n", kv.gid, kv.me, kv.config.Num, kv.getGivenShards(), kv.currentShards)
		if kv.shardsMoveOut() {
			kv.moveOutShards()
		}
		if kv.shardsValid() {
			finishConfigChangeArgs := ConfigChangeArgs{
				ClientId:     kv.gid,
				RequestId:    kv.config.Num,
				Config:       kv.config,
				ChangeFinish: true,
			}
			finishOp := Op{
				ClientId:  kv.gid,
				RequestId: kv.config.Num,
				Method:    MethodConfigChange,
				Args:      finishConfigChangeArgs,
			}
			kv.rf.Start(finishOp)
		}
		kv.mu.RUnlock()
		time.Sleep(time.Millisecond * CONFIG_POLLING_FIX_DELAY_MILLISECONDS)
	}
}

func (kv *ShardKV) shardsMoveOut() bool {
	givenShards := kv.getGivenShards()
	return len(kv.currentShards) > len(givenShards)
}

func (kv *ShardKV) prepareMoveoutKeyValueData(shards []int) map[string]string {
	data := make(map[string]string)
	for k, v := range kv.stateMachine {
		shard := key2shard(k)
		for _, s := range shards {
			if s == shard {
				data[k] = v
			}
		}
	}
	return data
}

func (kv *ShardKV) prepareMoveoutResponseData(shards []int) map[int]Op {
	lastResponse := make(map[int]Op)
	for clientId, resp := range kv.lastResponse {
		switch resp.Method {
		case MethodGet, MethodPut, MethodAppend:
			for i := 0; i < len(shards); i++ {
				if shards[i] == resp.Shard {
					lastResponse[clientId] = resp
				}
			}
		}
	}
	return lastResponse
}

func (kv *ShardKV) moveOutShards() {
	leaveShards := kv.getLeaveShards()
	for gid, shards := range leaveShards {
		data := kv.prepareMoveoutKeyValueData(shards)
		lastResponse := kv.prepareMoveoutResponseData(shards)
		args := ShardsAddArgs{
			ClientId:     kv.gid,
			RequestId:    kv.config.Num,
			GID:          kv.gid,
			Config:       kv.config,
			Shards:       shards,
			KeyValueData: data,
			LastResponse: lastResponse,
		}
		servers := kv.config.Groups[gid]
		success := false
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply ShardsAddReply
			ok := srv.Call("ShardKV.ShardsAdd", &args, &reply)
			// log.Printf("group %d, server %d, num: %d, call ShardsAdd, args:%+v\n", kv.gid, kv.me, kv.config.Num, args)
			if ok && reply.Err == OK {
				success = true
				break
			}
			if ok && reply.Err == ErrWrongGroup {
				break
			}
		}
		if success {
			removeArgs := ShardsRemoveArgs{
				ClientId:  gid,
				RequestId: kv.config.Num,
				GID:       gid,
				Config:    kv.config,
				Shards:    shards,
			}
			go kv.ShardsRemove(&removeArgs, &ShardsRemoveReply{})
		} else {
			log.Printf("group %d, server %d, num: %d, send shards failed, args: target group %d, shards %+v\n", kv.gid, kv.me, kv.config.Num, gid, args.Shards)
		}
	}
}

func (kv *ShardKV) printGroupInfo() {
	for {
		kv.mu.RLock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			log.Printf("group %d, leader %d, num: %d, still alive \n", kv.gid, kv.me, kv.config.Num)
		}
		kv.mu.RUnlock()
		time.Sleep(time.Second * 2)
	}
}
