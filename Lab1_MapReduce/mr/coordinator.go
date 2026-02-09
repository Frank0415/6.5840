package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu           sync.Mutex
	ReduceBucket int
	Workerids    []int
	NextWorkerId int
	mTasks       []Task
	rTasks       []Task
}

type Task struct {
	FilePath     string
	WorkId       int
	WorkType     int // 1 -> Map Work, 2 -> Reduce Work
	ReduceBucket int
	Status       int // 0 -> Not Assigned, 1 -> In Progress, 2 -> Completed
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.WorkerId = len(c.Workerids)
	c.Workerids = append(c.Workerids, reply.WorkerId)
	reply.TotalMap = len(c.mTasks)
	reply.TotalReduce = len(c.rTasks)
	return nil
}

func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.WorkType = 0
	return nil
}

func (c *Coordinator) FinishWork(args *FinishWorkArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReduceBucket: nReduce,
	}

	// First Split into nReduce intermediate buckets

	for i, file := range files {
		c.mTasks = append(c.mTasks, Task{
			FilePath: file,
			WorkId:   i,
			WorkType: 1,
		})
	}
	for i := range c.rTasks {
		c.rTasks = append(c.rTasks, Task{
			WorkId:       i,
			WorkType:     2,
			ReduceBucket: i,
		})
	}

	// Then design a range of Map and Reduce tasks for each worker to pick up.

	c.server()
	return &c
}
