package shardctrler

import (
	"log"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	JOIN                            = "Join"
	LEAVE                           = "Leave"
	MOVE                            = "Move"
	QUERY                           = "Query"
	FOR_LOOP_MAX_RETRIES            = 10
	FOR_LOOP_FIX_DELAY_MILLISECONDS = 10
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs                []Config // indexed by config num
	maxraftstate           int      // snapshot if log grows this big
	logReplayOverheadBytes int
	lastRequestId          map[int]int
	lastResponse           map[int]Op
	persister              *raft.Persister
	lastCommandIndex       int
}

type Op struct {
	// Your data here.
	RequestId int
	ClientId  int
	Method    string
	Args      interface{}
	Reply     interface{}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if lastRid, exist := sc.lastRequestId[args.ClientId]; exist && lastRid == args.RequestId {
		opReply, exist := sc.lastResponse[args.ClientId].Reply.(JoinReply)
		sc.mu.Unlock()
		if exist {
			reply.WrongLeader = opReply.WrongLeader
			reply.Err = opReply.Err
			return
		}
	}

	getOp := Op{
		Method:    JOIN,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Args:      *args,
	}

	_, _, isLeader := sc.rf.Start(getOp)

	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	// log.Printf("leader %d receive command: %d, requestId:%d, clientId:%d\n", kv.me, index, args.RequestId, args.ClientId)
	sc.mu.Unlock()
	sc.watiJoinReply(args, reply)
}

func (sc *ShardCtrler) watiJoinReply(args *JoinArgs, reply *JoinReply) {
	for i := 0; i < FOR_LOOP_MAX_RETRIES; i++ {
		sc.mu.Lock()

		rId, exist := sc.lastRequestId[args.ClientId]
		if exist && rId == args.RequestId {
			opReply := sc.lastResponse[args.ClientId].Reply.(JoinReply)
			reply.WrongLeader = opReply.WrongLeader
			reply.Err = opReply.Err
			sc.mu.Unlock()
			return
		} else {
			sc.mu.Unlock()
			time.Sleep(time.Millisecond * FOR_LOOP_FIX_DELAY_MILLISECONDS)
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {

	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if lastRid, exist := sc.lastRequestId[args.ClientId]; exist && lastRid == args.RequestId {
		opReply, exist := sc.lastResponse[args.ClientId].Reply.(LeaveReply)
		sc.mu.Unlock()
		if exist {
			reply.WrongLeader = opReply.WrongLeader
			reply.Err = opReply.Err
			return
		}
	}

	getOp := Op{
		Method:    LEAVE,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Args:      *args,
	}

	_, _, isLeader := sc.rf.Start(getOp)

	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	// log.Printf("leader %d receive command: %d, requestId:%d, clientId:%d\n", kv.me, index, args.RequestId, args.ClientId)
	sc.mu.Unlock()
	sc.waitLeaveReply(args, reply)
}

func (sc *ShardCtrler) waitLeaveReply(args *LeaveArgs, reply *LeaveReply) {
	for i := 0; i < FOR_LOOP_MAX_RETRIES; i++ {
		sc.mu.Lock()
		rId, exist := sc.lastRequestId[args.ClientId]
		if exist && rId == args.RequestId {
			opReply := sc.lastResponse[args.ClientId].Reply.(LeaveReply)
			reply.WrongLeader = opReply.WrongLeader
			reply.Err = opReply.Err
			sc.mu.Unlock()
			return
		} else {
			sc.mu.Unlock()
			time.Sleep(time.Millisecond * FOR_LOOP_FIX_DELAY_MILLISECONDS)
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if lastRid, exist := sc.lastRequestId[args.ClientId]; exist && lastRid == args.RequestId {
		opReply, exist := sc.lastResponse[args.ClientId].Reply.(MoveReply)
		sc.mu.Unlock()
		if exist {
			reply.WrongLeader = opReply.WrongLeader
			reply.Err = opReply.Err
			return
		}
	}

	getOp := Op{
		Method:    MOVE,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Args:      *args,
	}

	_, _, isLeader := sc.rf.Start(getOp)

	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	// log.Printf("leader %d receive command: %d, requestId:%d, clientId:%d\n", kv.me, index, args.RequestId, args.ClientId)
	sc.mu.Unlock()
	sc.waitMoveReply(args, reply)
}

func (sc *ShardCtrler) waitMoveReply(args *MoveArgs, reply *MoveReply) {
	for i := 0; i < FOR_LOOP_MAX_RETRIES; i++ {
		sc.mu.Lock()
		rId, exist := sc.lastRequestId[args.ClientId]
		if exist && rId == args.RequestId {
			opReply := sc.lastResponse[args.ClientId].Reply.(MoveReply)
			reply.WrongLeader = opReply.WrongLeader
			reply.Err = opReply.Err
			sc.mu.Unlock()
			return
		} else {
			sc.mu.Unlock()
			time.Sleep(time.Millisecond * FOR_LOOP_FIX_DELAY_MILLISECONDS)
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if lastRid, exist := sc.lastRequestId[args.ClientId]; exist && lastRid == args.RequestId {
		opReply, exist := sc.lastResponse[args.ClientId].Reply.(QueryReply)
		sc.mu.Unlock()
		if exist {
			reply.WrongLeader = opReply.WrongLeader
			reply.Err = opReply.Err
			reply.Config = opReply.Config
			return
		}
	}

	getOp := Op{
		Method:    QUERY,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Args:      *args,
	}

	_, _, isLeader := sc.rf.Start(getOp)

	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	// log.Printf("leader %d receive command: %d, requestId:%d, clientId:%d\n", kv.me, index, args.RequestId, args.ClientId)
	sc.mu.Unlock()
	sc.waitQueryReply(args, reply)
}

func (sc *ShardCtrler) waitQueryReply(args *QueryArgs, reply *QueryReply) {
	for i := 0; i < FOR_LOOP_MAX_RETRIES; i++ {
		sc.mu.Lock()
		rId, exist := sc.lastRequestId[args.ClientId]
		if exist && rId == args.RequestId {
			opReply := sc.lastResponse[args.ClientId].Reply.(QueryReply)
			reply.WrongLeader = opReply.WrongLeader
			reply.Err = opReply.Err
			reply.Config = opReply.Config
			sc.mu.Unlock()
			return
		} else {
			sc.mu.Unlock()
			time.Sleep(time.Millisecond * FOR_LOOP_FIX_DELAY_MILLISECONDS)
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.lastRequestId = make(map[int]int)
	sc.lastResponse = make(map[int]Op)
	// Your code here.
	go sc.applier()
	return sc
}

func (sc *ShardCtrler) applier() {
	for {
		msg := <-sc.applyCh
		sc.mu.Lock()
		if msg.CommandValid {
			if msg.CommandIndex == sc.lastCommandIndex {
				log.Printf("warning: duplicated command %+v\n", msg)
			} else if msg.CommandIndex > sc.lastCommandIndex+1 {
				log.Printf("warning: missing some command, now %d, last %d\n", msg.CommandIndex, sc.lastCommandIndex)
			} else if msg.CommandIndex < sc.lastCommandIndex {
				log.Printf("warning: outdated command, now %d, last %d\n", msg.CommandIndex, sc.lastCommandIndex)
			}
			sc.lastCommandIndex = msg.CommandIndex
			sc.applyMsg(&msg)
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) getLatestConfig() Config {
	latestCofig := len(sc.configs) - 1
	return sc.configs[latestCofig]
}

func (sc *ShardCtrler) distributeShardLeave(newConfig Config) [NShards]int {
	mpa := make(map[int][]int)
	oldShards := sc.getLatestConfig().Shards
	for i := 0; i < NShards; i++ {
		gid := oldShards[i]
		shards, exist := mpa[gid]
		if exist {
			shards = append(shards, i)
			mpa[gid] = shards
		} else {
			mpa[gid] = []int{i}
		}
	}

	sortedMpaKeys := make([]int, len(mpa))
	for k := range mpa {
		sortedMpaKeys = append(sortedMpaKeys, k)
	}
	sort.Ints(sortedMpaKeys)

	newGids := []int{}
	for k := range newConfig.Groups {
		newGids = append(newGids, k)
	}
	sort.Ints(newGids)
	// log.Printf("sharding variables, newGrid: %+v, mpa:%+v, %+v\n", newGids, mpa, oldShards)
	avg := NShards / len(newConfig.Groups)
	freeShard := []int{}
	for _, k := range sortedMpaKeys {
		v := mpa[k]
		exist := false
		for _, gid := range newGids {
			if k == gid {
				exist = true
				break
			}
		}
		shardsNum := len(v)
		if !exist {
			freeShard = append(freeShard, v...)
			delete(mpa, k)
		} else if shardsNum > avg+1 {
			freeShard = append(freeShard, v[avg+1:]...)
			v = v[:avg+1]
			mpa[k] = v
		}
	}

	for _, newGid := range newGids {
		for i := 0; i < avg; i++ {
			if len(freeShard) == 0 {
				break
			}
			shards := mpa[newGid]
			if len(shards) < avg {
				shard := freeShard[len(freeShard)-1:]
				freeShard = freeShard[:len(freeShard)-1]
				shards = append(shards, shard...)
				mpa[newGid] = shards
				oldShards[shard[0]] = newGid
			}
		}
	}

	for _, newGid := range newGids {
		if len(freeShard) == 0 {
			break
		}
		shards := mpa[newGid]
		if len(shards) < avg+1 {
			shard := freeShard[len(freeShard)-1:]
			freeShard = freeShard[:len(freeShard)-1]
			shards = append(shards, shard...)
			mpa[newGid] = shards
			oldShards[shard[0]] = newGid
		}
	}
	return oldShards

}

func (sc *ShardCtrler) distributeShardJoin(newConfig Config) [NShards]int {
	mpa := make(map[int][]int)
	oldShards := sc.getLatestConfig().Shards
	for i := 0; i < NShards; i++ {
		gid := oldShards[i]
		shards, exist := mpa[gid]
		if exist {
			shards = append(shards, i)
			mpa[gid] = shards
		} else {
			mpa[gid] = []int{i}
		}
	}
	sortedMpaKeys := make([]int, len(mpa))
	for k := range mpa {
		sortedMpaKeys = append(sortedMpaKeys, k)
	}
	sort.Ints(sortedMpaKeys)

	newGids := []int{}
	for k := range newConfig.Groups {
		newGids = append(newGids, k)
	}
	sort.Ints(newGids)
	// log.Printf("sharding variables, newGrid: %+v, mpa:%+v, %+v\n", newGids, mpa, oldShards)
	avg := NShards / len(newConfig.Groups)
	freeShard := []int{}
	for _, k := range sortedMpaKeys {
		v := mpa[k]
		exist := false
		for _, gid := range newGids {
			if k == gid {
				exist = true
				break
			}
		}
		shardsNum := len(v)
		if !exist {
			freeShard = append(freeShard, v...)
			delete(mpa, k)
		} else if shardsNum > avg+1 {
			freeShard = append(freeShard, v[avg+1:]...)
			v = v[:avg+1]
			mpa[k] = v
		}
	}

	// distribute shard
	for _, newGid := range newGids {
		for i := 0; i < avg; i++ {
			if len(freeShard) == 0 {
				break
			}
			shards := mpa[newGid]
			if len(shards) < avg {
				shard := freeShard[len(freeShard)-1:]
				freeShard = freeShard[:len(freeShard)-1]
				shards = append(shards, shard...)
				mpa[newGid] = shards
				oldShards[shard[0]] = newGid
			}
			// log.Printf("sharding variables, avg %d newGrid: %+v, mpa:%+v, %+v\n", avg, newGids, mpa, oldShards)
		}
	}

	// distribute extra free shard
	for _, newGid := range newGids {
		if len(freeShard) == 0 {
			break
		}
		shards := mpa[newGid]
		if len(shards) <= avg {
			shard := freeShard[len(freeShard)-1:]
			freeShard = freeShard[:len(freeShard)-1]
			shards = append(shards, shard...)
			mpa[newGid] = shards
			oldShards[shard[0]] = newGid
		}
	}
	// log.Printf("free shard %+v\n", freeShard)
	// log.Printf("sharding variables, avg %d newGrid: %+v, mpa:%+v, %+v\n", avg, newGids, mpa, oldShards)
	for _, newGid := range newGids {
		shards := mpa[newGid]
		if len(shards) < avg {
			for _, gid := range newGids {
				shards2 := mpa[gid]
				if len(shards2) > avg {
					shard := shards2[avg:]
					shards2 = shards2[:avg]
					mpa[gid] = shards2
					shards = append(shards, shard...)
					mpa[newGid] = shards
					oldShards[shard[0]] = newGid
					break
				}
			}
		}
	}
	// log.Println()
	// log.Printf("sharding variables, avg %d newGrid: %+v, mpa:%+v, %+v\n", avg, newGids, mpa, oldShards)
	// log.Println()
	return oldShards
}

func (sc *ShardCtrler) printConfig(config Config) {
	keys := []int{}
	for k, _ := range config.Groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	m := "num:%d ,groups: %+v, shards:%+v"
	log.Printf(m, config.Num, keys, config.Shards)
}

func (sc *ShardCtrler) applyMsg(msg *raft.ApplyMsg) {
	if msg.Command == nil {
		return
	}
	op, ok := msg.Command.(Op)
	if !ok {
		log.Printf("warning: command is not operation, %+v", op)
		return
	}
	// duplicate request
	if rId, exist := sc.lastRequestId[op.ClientId]; exist && rId == op.RequestId {
		return
	}
	switch op.Method {
	case JOIN:
		args, ok := op.Args.(JoinArgs)
		if !ok {
			log.Printf("warning: invalid join args %+v\n", args)
		}
		latestConfig := sc.getLatestConfig()
		newConfig := Config{
			Num:    latestConfig.Num + 1,
			Groups: make(map[int][]string),
		}
		for k, v := range latestConfig.Groups {
			newConfig.Groups[k] = v
		}
		for k, v := range args.Servers {
			newConfig.Groups[k] = v
		}
		copy(newConfig.Shards[:], latestConfig.Shards[:])
		// update shard

		newConfig.Shards = sc.distributeShardJoin(newConfig)
		sc.configs = append(sc.configs, newConfig)
		// if _, isLeader := sc.rf.GetState(); isLeader {
		// 	log.Println()
		// 	log.Printf("args: %+v\n", args)
		// 	sc.printConfig(latestConfig)
		// 	sc.printConfig(newConfig)
		// } else {
		// 	log.Println()
		// 	log.Printf("args: %+v\n", args)
		// 	sc.printConfig(latestConfig)
		// 	sc.printConfig(newConfig)
		// }

		reply := JoinReply{
			WrongLeader: false,
			Err:         OK,
		}
		op.Reply = reply
	case LEAVE:
		args, ok := op.Args.(LeaveArgs)
		if !ok {
			log.Printf("warning: invalid leave args %+v\n", args)
		}
		latestConfig := sc.getLatestConfig()
		newConfig := Config{
			Num:    latestConfig.Num + 1,
			Groups: make(map[int][]string),
		}
		for k, v := range latestConfig.Groups {
			exist := false
			for _, gid := range args.GIDs {
				if gid == k {
					exist = true
					break
				}
			}
			if !exist {
				newConfig.Groups[k] = v
			}
		}

		// update shard
		if len(newConfig.Groups) > 0 {

			copy(newConfig.Shards[:], latestConfig.Shards[:])
			newConfig.Shards = sc.distributeShardLeave(newConfig)
		}

		sc.configs = append(sc.configs, newConfig)
		// if _, isLeader := sc.rf.GetState(); isLeader {
		// 	log.Println()
		// 	log.Printf("args: %+v\n", args)
		// 	sc.printConfig(latestConfig)
		// 	sc.printConfig(newConfig)
		// } else {
		// 	log.Println()
		// 	log.Printf("args: %+v\n", args)
		// 	sc.printConfig(latestConfig)
		// 	sc.printConfig(newConfig)
		// }
		reply := LeaveReply{
			WrongLeader: false,
			Err:         OK,
		}
		op.Reply = reply
	case MOVE:
		args, ok := op.Args.(MoveArgs)
		if !ok {
			log.Printf("warning: invalid move args %+v\n", args)
		}
		latestConfig := sc.getLatestConfig()
		newConfig := Config{
			Num:    latestConfig.Num + 1,
			Groups: make(map[int][]string),
		}
		for k, v := range latestConfig.Groups {
			newConfig.Groups[k] = v
		}
		copy(newConfig.Shards[:], latestConfig.Shards[:])
		// update shard
		newConfig.Shards[args.Shard] = args.GID
		sc.configs = append(sc.configs, newConfig)
		reply := MoveReply{
			WrongLeader: false,
			Err:         OK,
		}
		op.Reply = reply
	case QUERY:
		args, ok := op.Args.(QueryArgs)
		if !ok {
			log.Printf("warning: invalid query args %+v\n", args)
		}
		reply := QueryReply{
			WrongLeader: false,
			Err:         OK,
		}
		if args.Num == LATEST_CONFIGURATION {
			reply.Config = sc.getLatestConfig()
		} else {
			reply.Config = sc.configs[args.Num]
		}
		op.Reply = reply
		// if _, isLeader := sc.rf.GetState(); isLeader {
		// 	log.Println()
		// 	log.Printf("query args: %+v\n", args)
		// 	sc.printConfig(reply.Config)
		// } else {
		// 	log.Println()
		// 	log.Printf("query args: %+v\n", args)
		// 	sc.printConfig(reply.Config)
		// }
	default:
		log.Printf("warning: unknown command %+v\n", msg)
	}

	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.lastResponse[op.ClientId] = op
}
