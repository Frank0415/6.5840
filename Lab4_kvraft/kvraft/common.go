package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId     int64
	ClientId int64
}

type PutAppendReply struct {
	Err Err
	Value string
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

type RequestInfo struct {
	OpId     int64
	ClientId int64
}

type ReturnInfo struct {
	Value string
	OpId  int64
}
