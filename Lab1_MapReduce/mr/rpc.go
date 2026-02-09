package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	WorkerId    int
	TotalMap    int
	TotalReduce int
}

type GetWorkArgs struct {
	WorkerId int
}

// The WorkReply struct
// WorkType: 0 -> No Work, 1 -> Map Work, 2 -> Reduce Work
// OutFile: the output file for Map work and Reduce work, they all need sth. to write to.
// ReduceBucket: the slice number for Reduce work.
type GetWorkReply struct {
	WorkType     int
	WorkId       int
	ReduceBucket int
	Path         string
}

type FinishWorkArgs struct {
	outPath  []string
	WorkerId int
	WorkId   int
	Status   int // 1 -> Map Work, 2 -> Reduce Work
	WorkType int // 1 -> Map Work, 2 -> Reduce Work
}

type FinishWorkReply struct {
	Ack bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
