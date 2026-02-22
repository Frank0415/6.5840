package shardctrler

import (
    "Lab5_shard/labgob"
    "Lab5_shard/labrpc"
    "Lab5_shard/raft"
    "sort"
    "sync"
    "sync/atomic"
    "time"
)

type ShardCtrler struct {
    mu      sync.Mutex
    me      int
    rf      *raft.Raft
    applyCh chan raft.ApplyMsg
    dead    atomic.Bool // set by Kill()

    configNum int
    configs   []Config // indexed by config num
    waitCh    map[int]chan Op
}

const (
    Join  = "Join"
    Leave = "Leave"
    Move  = "Move"
    Query = "Query"
)

type Op struct {
    Type    string
    Servers map[int][]string // For Join
    GIDs    []int            // For Leave
    Shard   int              // For Move
    GID     int              // For Move
    Num     int              // For Query
    ReqId   int64            // Unique ID to prevent index collisions
}

func (sc *ShardCtrler) createNextConfig() Config {
    lastConfig := sc.configs[len(sc.configs)-1]
    nextConfig := Config{
        Num:    lastConfig.Num + 1,
        Shards: lastConfig.Shards, // Arrays are copied by value
        Groups: make(map[int][]string),
    }

    // Deep copy the groups map
    for gid, servers := range lastConfig.Groups {
        nextConfig.Groups[gid] = append([]string{}, servers...)
    }

    return nextConfig
}

func rebalance(config *Config) {
    if len(config.Groups) == 0 {
        for i := range len(config.Shards) {
            config.Shards[i] = 0
        }
        return
    }

    // 1. Group shards by GID
    gidToShards := make(map[int][]int)
    var gids []int
    for gid := range config.Groups {
        gidToShards[gid] = make([]int, 0)
        gids = append(gids, gid)
    }
    sort.Ints(gids) // Sort to ensure deterministic iteration across all Raft peers

    // 2. Find unassigned shards
    var unassigned []int
    for shard, gid := range config.Shards {
        if _, exists := config.Groups[gid]; exists {
            gidToShards[gid] = append(gidToShards[gid], shard)
        } else {
            unassigned = append(unassigned, shard)
        }
    }

    // 3. Assign unassigned shards to the GID with the minimum shards
    for _, shard := range unassigned {
        minGid := gids[0]
        for _, gid := range gids {
            if len(gidToShards[gid]) < len(gidToShards[minGid]) {
                minGid = gid
            }
        }
        config.Shards[shard] = minGid
        gidToShards[minGid] = append(gidToShards[minGid], shard)
    }

    // 4. Balance shards among valid GIDs
    for {
        maxGid, minGid := gids[0], gids[0]
        for _, gid := range gids {
            if len(gidToShards[gid]) > len(gidToShards[maxGid]) {
                maxGid = gid
            }
            if len(gidToShards[gid]) < len(gidToShards[minGid]) {
                minGid = gid
            }
        }

        // If the difference between max and min is <= 1, it's balanced
        if len(gidToShards[maxGid])-len(gidToShards[minGid]) <= 1 {
            break
        }

        // Move one shard from maxGid to minGid
        shard := gidToShards[maxGid][0]
        gidToShards[maxGid] = gidToShards[maxGid][1:]
        gidToShards[minGid] = append(gidToShards[minGid], shard)
        config.Shards[shard] = minGid
    }
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
    reqId := time.Now().UnixNano()
    op := Op{Type: Join, Servers: args.Servers, ReqId: reqId}
    index, _, isLeader := sc.rf.Start(op)
    if !isLeader {
        reply.WrongLeader = true
        return
    }

    ch := sc.getWaitCh(index)
    defer func() {
        sc.mu.Lock()
        delete(sc.waitCh, index)
        sc.mu.Unlock()
    }()

    opApplied := <-ch
    if opApplied.ReqId != reqId {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK
    }
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
    reqId := time.Now().UnixNano()
    op := Op{Type: Leave, GIDs: args.GIDs, ReqId: reqId}
    index, _, isLeader := sc.rf.Start(op)
    if !isLeader {
        reply.WrongLeader = true
        return
    }

    ch := sc.getWaitCh(index)
    defer func() {
        sc.mu.Lock()
        delete(sc.waitCh, index)
        sc.mu.Unlock()
    }()

    opApplied := <-ch
    if opApplied.ReqId != reqId {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK
    }
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
    reqId := time.Now().UnixNano()
    op := Op{Type: Move, Shard: args.Shard, GID: args.GID, ReqId: reqId}
    index, _, isLeader := sc.rf.Start(op)
    if !isLeader {
        reply.WrongLeader = true
        return
    }

    ch := sc.getWaitCh(index)
    defer func() {
        sc.mu.Lock()
        delete(sc.waitCh, index)
        sc.mu.Unlock()
    }()

    opApplied := <-ch
    if opApplied.ReqId != reqId {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK
    }
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
    reqId := time.Now().UnixNano()
    op := Op{Type: Query, Num: args.Num, ReqId: reqId}
    index, _, isLeader := sc.rf.Start(op)
    if !isLeader {
        reply.WrongLeader = true
        return
    }

    ch := sc.getWaitCh(index)
    defer func() {
        sc.mu.Lock()
        delete(sc.waitCh, index)
        sc.mu.Unlock()
    }()

    opApplied := <-ch
    if opApplied.ReqId != reqId {
        reply.WrongLeader = true
    } else {
        reply.WrongLeader = false
        reply.Err = OK

        sc.mu.Lock()
        if args.Num == -1 || args.Num >= len(sc.configs) {
            reply.Config = sc.configs[len(sc.configs)-1]
        } else {
            reply.Config = sc.configs[args.Num]
        }
        sc.mu.Unlock()
    }
}

func (sc *ShardCtrler) Kill() {
    sc.dead.Store(true)
    sc.rf.Kill()
}

func (sc *ShardCtrler) Raft() *raft.Raft {
    return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
    sc := new(ShardCtrler)
    sc.me = me
    sc.dead.Store(true)

    sc.configs = make([]Config, 1)
    sc.configs[0].Groups = map[int][]string{}
    sc.waitCh = make(map[int]chan Op)

    labgob.Register(Op{})
    sc.applyCh = make(chan raft.ApplyMsg)
    sc.rf = raft.Make(servers, me, persister, sc.applyCh)

    go sc.apply()

    return sc
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
    sc.mu.Lock()
    defer sc.mu.Unlock()
    if _, ok := sc.waitCh[index]; !ok {
        sc.waitCh[index] = make(chan Op, 1)
    }
    return sc.waitCh[index]
}

func (sc *ShardCtrler) apply() {
    for msg := range sc.applyCh {
        if msg.CommandValid {
            op := msg.Command.(Op)

            sc.mu.Lock()

            if op.Type == Join {
                // Idempotency check: only append if it actually adds new groups
                isDup := true
                lastConfig := sc.configs[len(sc.configs)-1]
                for gid := range op.Servers {
                    if _, ok := lastConfig.Groups[gid]; !ok {
                        isDup = false
                        break
                    }
                }
                if !isDup {
                    nextConfig := sc.createNextConfig()
                    for gid, servers := range op.Servers {
                        nextConfig.Groups[gid] = append([]string{}, servers...)
                    }
                    rebalance(&nextConfig)
                    sc.configs = append(sc.configs, nextConfig)
                }
            } else if op.Type == Leave {
                // Idempotency check: only append if it actually removes existing groups
                isDup := true
                lastConfig := sc.configs[len(sc.configs)-1]
                for _, gid := range op.GIDs {
                    if _, ok := lastConfig.Groups[gid]; ok {
                        isDup = false
                        break
                    }
                }
                if !isDup {
                    nextConfig := sc.createNextConfig()
                    for _, gid := range op.GIDs {
                        delete(nextConfig.Groups, gid)
                    }
                    rebalance(&nextConfig)
                    sc.configs = append(sc.configs, nextConfig)
                }
            } else if op.Type == Move {
                lastConfig := sc.configs[len(sc.configs)-1]
                if lastConfig.Shards[op.Shard] != op.GID {
                    nextConfig := sc.createNextConfig()
                    nextConfig.Shards[op.Shard] = op.GID
                    sc.configs = append(sc.configs, nextConfig)
                }
            }

            // Notify the waiting RPC handler
            if ch, ok := sc.waitCh[msg.CommandIndex]; ok {
                ch <- op
            }

            sc.mu.Unlock()
        }
    }
}