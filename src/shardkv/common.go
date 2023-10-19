package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	MethodConfigChange = "ConfigChange"
	MethodAddShards    = "AddShards"
	MethodRemoveShards = "RemoveShards"
	MethodPut          = "Put"
	MethodAppend       = "Append"
	MethodGet          = "Get"
	LatestConfig       = -1
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ClientId  int
	RequestId int
	ConfigNum int
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId  int
	RequestId int
	ConfigNum int
	Key       string
	// You'll have to add definitions here.
}

type ConfigChangeArgs struct {
	ClientId     int
	RequestId    int
	Config       shardctrler.Config
	PrevConfig   shardctrler.Config
	ChangeFinish bool
}

type ConfigChangeReply struct {
	Err Err
}

type ShardsAddArgs struct {
	ClientId  int
	RequestId int
	GID       int
	Config    shardctrler.Config
	Shards    []int
	Data      map[string]string
}

type ShardsAddReply struct {
	Err Err
}

type ShardsRemoveArgs struct {
	ClientId  int
	RequestId int
	GID       int
	Config    shardctrler.Config
	Shards    []int
}

type ShardsRemoveReply struct {
	Err Err
}

type GetReply struct {
	Err   Err
	Value string
}
