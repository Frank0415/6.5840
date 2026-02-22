package shardkv

import "Lab5_shard/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId     int64
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpId     int64
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type FetchShardArgs struct {
	ShardId   int
	ConfigNum int
}

type FetchShardReply struct {
	Err       Err
	KV        map[string]string
	ClientSeq map[int64]int64
}

type CheckShardArgs struct {
	ShardId   int
	ConfigNum int
}

type CheckShardReply struct {
	Err Err
}
