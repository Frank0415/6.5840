package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	data      map[string]string
	IdtoIndex map[int64]int
	Response  []ReturnInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.data[args.Key]

	if index, ok := kv.IdtoIndex[args.ClientId]; ok {
		if ret := kv.Response[index]; ret.OpId < args.OpId {
			kv.Response[index] = ReturnInfo{Value: "", OpId: args.OpId}
		}
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data[args.Key] = args.Value

	// Index := kv.getIndex(args.ClientId)
	// if ret := kv.Response[Index]; ret.OpId < args.OpId {
	// 	kv.Response[Index] = ReturnInfo{Value: "", OpId: args.OpId}
	// }

	if index, ok := kv.IdtoIndex[args.ClientId]; ok {
		if ret := kv.Response[index]; ret.OpId < args.OpId {
			kv.Response[index] = ReturnInfo{Value: "", OpId: args.OpId}
		}
	}
	
	if reply != nil {
		reply.Value = ""
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var tmp string

	Index := kv.getIndex(args.ClientId)
	if ret := kv.Response[Index]; ret.OpId < args.OpId {
		tmp = kv.data[args.Key]
		kv.data[args.Key] = tmp + args.Value
		kv.Response[Index] = ReturnInfo{Value: tmp, OpId: args.OpId}
	} else {
		tmp = kv.Response[Index].Value
	}

	if reply != nil {
		reply.Value = tmp
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.Response = make([]ReturnInfo, 0)
	kv.IdtoIndex = make(map[int64]int)
	kv.data = make(map[string]string)
	// You may need initialization code here.

	return kv
}

func (kv *KVServer) getIndex(ClientId int64) int {
	if index, ok := kv.IdtoIndex[ClientId]; ok {
		return index
	} else {
		index := len(kv.Response)
		kv.IdtoIndex[ClientId] = index
		kv.Response = append(kv.Response, ReturnInfo{})
		return index
	}
}
