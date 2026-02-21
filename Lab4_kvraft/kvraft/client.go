package kvraft

import (
	"Lab4_kvraft/labrpc"
	"crypto/rand"
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	ClientId int64
	OpId     atomic.Int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ClientId = nrand()
	ck.leaderId = 0
	ck.OpId.Store(0)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	opid := ck.OpId.Add(1)
	args := GetArgs{Key: key, ClientId: ck.ClientId, OpId: opid}

	for {
		var reply GetReply
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	opid := ck.OpId.Add(1)
	args := PutAppendArgs{Key: key, Value: value, OpId: opid, ClientId: ck.ClientId}

	for {
		var reply PutAppendReply
		ok := ck.servers[ck.leaderId].Call("KVServer."+op, &args, &reply)
		if ok && reply.Err == OK {
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
