package shardkv

import (
	"Lab5_shard/labgob"
	"Lab5_shard/labrpc"
	"Lab5_shard/raft"
	"Lab5_shard/shardctrler"
	"bytes"
	"sync"
	"time"
)

type ShardStatus int

const (
	Serving ShardStatus = iota
	Pulling
	Pushing
)

type Shard struct {
	KV     map[string]string
	Status ShardStatus
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	OpId     int64
	Key      string
	Value    string
	Type     string // "Get", "Put", "Append", "ConfigChange", "InsertShard"

	// For ConfigChange
	Config shardctrler.Config

	// For InsertShard
	ShardId   int
	ConfigNum int
	KV        map[string]string
	ClientSeq map[int64]int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	mck *shardctrler.Clerk

	config     shardctrler.Config
	lastConfig shardctrler.Config

	shards    [shardctrler.NShards]Shard
	clientSeq map[int]map[int64]int64 // ShardID -> ClientID -> SeqNum

	waitCh     map[int]chan Op
	applyIndex int
}

func (kv *ShardKV) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.waitCh[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.waitCh[index] = ch
	}
	return ch
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.shards[shardId].Status != Serving {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
	}()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClientId == op.ClientId && appliedOp.OpId == op.OpId {
			kv.mu.Lock()
			if kv.config.Shards[shardId] != kv.gid || kv.shards[shardId].Status != Serving {
				reply.Err = ErrWrongGroup
			} else {
				if val, ok := kv.shards[shardId].KV[op.Key]; ok {
					reply.Value = val
					reply.Err = OK
				} else {
					reply.Err = ErrNoKey
				}
			}
			kv.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.shards[shardId].Status != Serving {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitCh, index)
		kv.mu.Unlock()
	}()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClientId == op.ClientId && appliedOp.OpId == op.OpId {
			kv.mu.Lock()
			if kv.config.Shards[shardId] != kv.gid || kv.shards[shardId].Status != Serving {
				reply.Err = ErrWrongGroup
			} else {
				reply.Err = OK
			}
			kv.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.shards[i] = Shard{
			KV:     make(map[string]string),
			Status: Serving,
		}
	}
	kv.clientSeq = make(map[int]map[int64]int64)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.clientSeq[i] = make(map[int64]int64)
	}
	kv.waitCh = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.decodeSnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.configPoller()
	go kv.migrationPoller()
	go kv.gcPoller()

	return kv
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.config)
	e.Encode(kv.lastConfig)
	e.Encode(kv.shards)
	e.Encode(kv.clientSeq)
	return w.Bytes()
}

func (kv *ShardKV) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var config shardctrler.Config
	var lastConfig shardctrler.Config
	var shards [shardctrler.NShards]Shard
	var clientSeq map[int]map[int64]int64
	if d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&shards) != nil ||
		d.Decode(&clientSeq) != nil {
		// log.Fatalf("decode error")
	} else {
		kv.config = config
		kv.lastConfig = lastConfig
		kv.shards = shards
		kv.clientSeq = clientSeq
	}
}

func (kv *ShardKV) applier() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()

			kv.applyIndex = msg.CommandIndex

			if op.Type == "ConfigChange" {
				if op.Config.Num == kv.config.Num+1 {
					kv.lastConfig = kv.config
					kv.config = op.Config

					for i := 0; i < shardctrler.NShards; i++ {
						if kv.config.Shards[i] == kv.gid && kv.lastConfig.Shards[i] != kv.gid {
							if kv.lastConfig.Num == 0 {
								kv.shards[i].Status = Serving
							} else {
								kv.shards[i].Status = Pulling
							}
						} else if kv.config.Shards[i] != kv.gid && kv.lastConfig.Shards[i] == kv.gid {
							kv.shards[i].Status = Pushing
						}
					}
				}
			} else if op.Type == "InsertShard" {
				if op.ConfigNum == kv.config.Num && kv.shards[op.ShardId].Status == Pulling {
					for k, v := range op.KV {
						kv.shards[op.ShardId].KV[k] = v
					}
					for k, v := range op.ClientSeq {
						if lastId, ok := kv.clientSeq[op.ShardId][k]; !ok || v > lastId {
							kv.clientSeq[op.ShardId][k] = v
						}
					}
					kv.shards[op.ShardId].Status = Serving
				}
			} else if op.Type == "DeleteShard" {
				if op.ConfigNum <= kv.config.Num && kv.shards[op.ShardId].Status == Pushing {
					kv.shards[op.ShardId].KV = make(map[string]string)
					kv.clientSeq[op.ShardId] = make(map[int64]int64)
					kv.shards[op.ShardId].Status = Serving
				}
			} else {
				shardId := key2shard(op.Key)
				if kv.config.Shards[shardId] == kv.gid && kv.shards[shardId].Status == Serving {
					if op.Type != "Get" {
						lastId, ok := kv.clientSeq[shardId][op.ClientId]
						if !ok || op.OpId > lastId {
							switch op.Type {
							case "Put":
								kv.shards[shardId].KV[op.Key] = op.Value
							case "Append":
								kv.shards[shardId].KV[op.Key] += op.Value
							}
							kv.clientSeq[shardId][op.ClientId] = op.OpId
						}
					}
				}
			}

			if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
				ch <- op
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.rf.Snapshot(msg.CommandIndex, kv.encodeSnapshot())
			}

			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if msg.SnapshotIndex > kv.applyIndex {
				kv.decodeSnapshot(msg.Snapshot)
				kv.applyIndex = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) configPoller() {
	for {
		kv.mu.Lock()
		canPoll := true
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.shards[i].Status == Pulling {
				canPoll = false
				break
			}
		}
		nextNum := kv.config.Num + 1
		kv.mu.Unlock()

		if canPoll {
			nextConfig := kv.mck.Query(nextNum)
			if nextConfig.Num == nextNum {
				op := Op{
					Type:   "ConfigChange",
					Config: nextConfig,
				}
				kv.rf.Start(op)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) migrationPoller() {
	for {
		kv.mu.Lock()
		var pullingShards []int
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.shards[i].Status == Pulling {
				pullingShards = append(pullingShards, i)
			}
		}
		lastConfig := kv.lastConfig
		configNum := kv.config.Num
		kv.mu.Unlock()

		for _, shardId := range pullingShards {
			gid := lastConfig.Shards[shardId]
			if servers, ok := lastConfig.Groups[gid]; ok {
				go func(shardId int, configNum int, servers []string) {
					args := FetchShardArgs{
						ShardId:   shardId,
						ConfigNum: configNum,
					}
					for _, server := range servers {
						srv := kv.make_end(server)
						var reply FetchShardReply
						ok := srv.Call("ShardKV.FetchShard", &args, &reply)
						if ok && reply.Err == OK {
							op := Op{
								Type:      "InsertShard",
								ShardId:   shardId,
								ConfigNum: configNum,
								KV:        reply.KV,
								ClientSeq: reply.ClientSeq,
							}
							kv.rf.Start(op)
							break
						}
					}
				}(shardId, configNum, servers)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) gcPoller() {
	for {
		kv.mu.Lock()
		var pushingShards []int
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.shards[i].Status == Pushing {
				pushingShards = append(pushingShards, i)
			}
		}
		config := kv.config
		kv.mu.Unlock()

		for _, shardId := range pushingShards {
			gid := config.Shards[shardId]
			if servers, ok := config.Groups[gid]; ok {
				go func(shardId int, configNum int, servers []string) {
					args := CheckShardArgs{
						ShardId:   shardId,
						ConfigNum: configNum,
					}
					for _, server := range servers {
						srv := kv.make_end(server)
						var reply CheckShardReply
						ok := srv.Call("ShardKV.CheckShard", &args, &reply)
						if ok && reply.Err == OK {
							op := Op{
								Type:      "DeleteShard",
								ShardId:   shardId,
								ConfigNum: configNum,
							}
							kv.rf.Start(op)
							break
						}
					}
				}(shardId, config.Num, servers)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) FetchShard(args *FetchShardArgs, reply *FetchShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrWrongGroup // Or some other error to indicate not ready
		return
	}

	reply.KV = make(map[string]string)
	for k, v := range kv.shards[args.ShardId].KV {
		reply.KV[k] = v
	}

	reply.ClientSeq = make(map[int64]int64)
	for k, v := range kv.clientSeq[args.ShardId] {
		reply.ClientSeq[k] = v
	}

	reply.Err = OK
}

func (kv *ShardKV) CheckShard(args *CheckShardArgs, reply *CheckShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num >= args.ConfigNum && kv.shards[args.ShardId].Status == Serving {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongGroup
	}
}
