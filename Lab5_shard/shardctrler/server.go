package shardctrler


import "Lab5_shard/raft"
import "Lab5_shard/labrpc"
import "sync"
import "Lab5_shard/labgob"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configNum int
	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here. 
	sc.configs = append(sc.configs, Config{})
	sc.configs[len(sc.configs)-1].Groups = args.Servers
	sc.configs[len(sc.configs)-1].Num = len(sc.configs) - 1
	sc.configNum = len(sc.configs) - 1
	// TODO: reassign shards
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.configs = append(sc.configs, Config{})
	sc.configs[len(sc.configs)-1].Num = len(sc.configs) - 1
	sc.configs[len(sc.configs)-1].Groups = sc.configs[len(sc.configs)-2].Groups
	for _, gid := range args.GIDs {
		delete(sc.configs[len(sc.configs)-1].Groups, gid)
	}
	sc.configNum = len(sc.configs) - 1
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.configs = append(sc.configs, Config{})
	sc.configs[len(sc.configs)-1].Num = len(sc.configs) - 1
	sc.configs[len(sc.configs)-1].Groups = sc.configs[len(sc.configs)-2].Groups
	sc.configs[len(sc.configs)-1].Shards[args.Shard] = args.GID
	sc.configNum = len(sc.configs) - 1
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if args.Num == -1 || args.Num >= len(sc.configs) {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
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
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
