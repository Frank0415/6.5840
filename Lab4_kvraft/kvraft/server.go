package kvraft

import (
	"Lab4_kvraft/labgob"
	"Lab4_kvraft/labrpc"
	"Lab4_kvraft/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	ClientId int64
	OpId     int64
	Key      string
	Value    string
	Type     string // "Get", "Put" or "Append"
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	data        map[string]string
	lastApplied map[int64]int64
	waitCh      map[int]chan Op
	applyIndex  int
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.waitCh[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.waitCh[index] = ch
	}
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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
			reply.Value = kv.data[op.Key]
			reply.Err = OK
			kv.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     "Put",
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	kv.submitOp(op, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     "Append",
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	kv.submitOp(op, reply)
}

func (kv *KVServer) submitOp(op Op, reply *PutAppendReply) {
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
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
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
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.waitCh = make(map[int]chan Op)

	kv.readSnapshot(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()

			kv.applyIndex = msg.CommandIndex

			if op.Type != "Get" {
				lastId, ok := kv.lastApplied[op.ClientId]
				if !ok || op.OpId > lastId {
					kv.applyIndex = msg.CommandIndex
					switch op.Type {
					case "Put":
						kv.data[op.Key] = op.Value
					case "Append":
						kv.data[op.Key] += op.Value
					default:
						log.Fatalf("Unknown operation type: %s", op.Type)
					}
					kv.lastApplied[op.ClientId] = op.OpId
				}
			}

			if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
				ch <- op
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate*4/5 {
				kv.MakeSnapshot()
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) MakeSnapshot() {
	// DO NOT LOCK as it is called from applier which already holds the lock
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.lastApplied)
	e.Encode(kv.applyIndex)
	data := w.Bytes()
	kv.rf.Snapshot(kv.applyIndex, data)
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var lastApplied map[int64]int64
	var CommandIndex int
	if d.Decode(&data) != nil || d.Decode(&lastApplied) != nil || d.Decode(&CommandIndex) != nil {
		log.Fatalf("Failed to read snapshot")
	} else {
		kv.data = data
		kv.lastApplied = lastApplied
		kv.applyIndex = CommandIndex
	}
}
