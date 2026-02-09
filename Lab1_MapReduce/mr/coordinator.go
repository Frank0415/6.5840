package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu               sync.Mutex
	ReduceBucket     int
	Workerids        []int
	NextWorkerId     int
	mTasks           []Task
	rTasks           []Task
	slowMTasks       []Task
	slowRTasks       []Task
	nCompletedMap    int
	nCompletedReduce int
	WorkerLastSeen   map[int]time.Time // Added to track worker health
}

type Task struct {
	FilePath     string
	WorkId       int
	WorkType     int // 1 -> Map Work, 2 -> Reduce Work
	ReduceBucket int
	Status       int   // 0 -> Not Assigned, 1 -> In Progress, 2 -> Completed
	SlowLinked   *Task // If this task is considered slow, we link it to its original task for easy lookup
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
	c.WorkerLastSeen[reply.WorkerId] = time.Now()
	log.Printf("Worker %v registered. Total workers: %v\n", reply.WorkerId, len(c.Workerids))
	return nil
}

func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Check for available Slow Map Tasks
	for i := range c.slowMTasks {
		if c.slowMTasks[i].Status == 0 {
			c.slowMTasks[i].Status = 1 // only change to 1 is enough, we will never change it to 2
			reply.WorkType = 1
			reply.WorkId = c.slowMTasks[i].WorkId
			reply.Path = c.slowMTasks[i].FilePath
			log.Printf("Assigned Slow Map task %v to Worker %v\n", reply.WorkId, args.WorkerId)
			return nil
		}
	}

	// 2. Check for available Map tasks
	for i := range c.mTasks {
		if c.mTasks[i].Status == 0 {
			c.mTasks[i].Status = 1
			reply.WorkType = 1
			reply.WorkId = c.mTasks[i].WorkId
			reply.Path = c.mTasks[i].FilePath
			log.Printf("Assigned Map task %v to Worker %v\n", reply.WorkId, args.WorkerId)
			return nil
		}
	}

	if c.nCompletedMap < len(c.mTasks) {
		reply.WorkType = 0
		return nil
	}

	// 3. Check for available Slow Reduce Tasks
	for i := range c.slowRTasks {
		if c.slowRTasks[i].Status == 0 {
			c.slowRTasks[i].Status = 1
			reply.WorkType = 2
			reply.WorkId = c.slowRTasks[i].WorkId
			reply.ReduceBucket = c.slowRTasks[i].ReduceBucket
			log.Printf("Assigned Slow Reduce task %v to Worker %v\n", reply.WorkId, args.WorkerId)
			return nil
		}
	}

	// 4. Check for available Reduce tasks
	for i := range c.rTasks {
		if c.rTasks[i].Status == 0 {
			c.rTasks[i].Status = 1
			reply.WorkType = 2
			reply.WorkId = c.rTasks[i].WorkId
			reply.ReduceBucket = c.rTasks[i].ReduceBucket
			log.Printf("Assigned Reduce task %v to Worker %v\n", reply.WorkId, args.WorkerId)
			return nil
		}
	}

	reply.WorkType = 0
	return nil
}

func (c *Coordinator) FinishWork(args *FinishWorkArgs, reply *FinishWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.WorkerLastSeen[args.WorkerId]; !exists {
		log.Printf("Unknown Worker %v reported work completion, ignoring\n", args.WorkerId)
		return nil
	}
	switch args.WorkType {
	case 1:
		// We use at least once semantics, so we may receive duplicate, but we only count the first completion.
		if c.mTasks[args.WorkId].Status != 2 {
			c.mTasks[args.WorkId].Status = 2
			if c.mTasks[args.WorkId].SlowLinked != nil {
				c.mTasks[args.WorkId].SlowLinked.Status = 2
			}
			c.nCompletedMap++
		}
		log.Printf("Worker %v completed Map task %v. Total completed Map tasks: %v\n", args.WorkerId, args.WorkId, c.nCompletedMap)
	case 2:
		if c.rTasks[args.WorkId].Status != 2 {
			c.rTasks[args.WorkId].Status = 2
			if c.rTasks[args.WorkId].SlowLinked != nil {
				c.rTasks[args.WorkId].SlowLinked.Status = 2
			}
			c.nCompletedReduce++
		}
		log.Printf("Worker %v completed Reduce task %v. Total completed Reduce tasks: %v\n", args.WorkerId, args.WorkId, c.nCompletedReduce)
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()

	currTime := time.Now()
	for workerId, lastSeen := range c.WorkerLastSeen {
		if currTime.Sub(lastSeen) > 5*time.Second {
			log.Printf("Worker %v is considered failed (last seen %v seconds ago)\n", workerId, currTime.Sub(lastSeen).Seconds())
			// Mark any in-progress tasks assigned to this worker as not assigned
			for i := range c.mTasks {
				if c.mTasks[i].Status == 1 {
					c.mTasks[i].Status = 0
				}
			}
			for i := range c.rTasks {
				if c.rTasks[i].Status == 1 {
					c.rTasks[i].Status = 0
				}
			}
			delete(c.WorkerLastSeen, workerId)
		}
	}

	// If all Reduce tasks are completed, the whole job is done.
	log.Printf("Checking if job is done: %v/%v Map tasks completed, %v/%v Reduce tasks completed\n", c.nCompletedMap, len(c.mTasks), c.nCompletedReduce, len(c.rTasks))
	return c.nCompletedReduce == len(c.rTasks)
}

func (c *Coordinator) CheckHealth(args *CheckHealthArgs, reply *CheckHealthReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Update the last seen time for the worker
	if _, exists := c.WorkerLastSeen[args.WorkerId]; !exists {
		log.Printf("Unknown Worker %v sent heartbeat\n", args.WorkerId)
		reply.Ack = false
	}

	c.WorkerLastSeen[args.WorkerId] = time.Now()
	log.Printf("Worker %v heartbeat received. WorkMsec: %v\n", args.WorkerId, args.WorkMsec)
	reply.Ack = true

	// We consider as a slow task
	if args.WorkMsec > 5000 {
		log.Printf("Worker %v is considered slow (WorkMsec %v)\n", args.WorkerId, args.WorkMsec)
		// Mark any in-progress tasks assigned to this worker as slow
		for i := range c.mTasks {
			if c.mTasks[i].Status == 1 {
				c.slowMTasks = append(c.slowMTasks, c.mTasks[i])
				c.slowMTasks[len(c.slowMTasks)-1].Status = 0
				c.mTasks[i].SlowLinked = &c.slowMTasks[len(c.slowMTasks)-1]
			}
		}
		for i := range c.rTasks {
			if c.rTasks[i].Status == 1 {
				c.slowRTasks = append(c.slowRTasks, c.rTasks[i])
				c.slowRTasks[len(c.slowRTasks)-1].Status = 0
				c.rTasks[i].SlowLinked = &c.slowRTasks[len(c.slowRTasks)-1]
			}
		}
	}

	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReduceBucket:     nReduce,
		nCompletedMap:    0,
		nCompletedReduce: 0,
		WorkerLastSeen:   make(map[int]time.Time),
	}

	// First Split into nReduce intermediate buckets
	// Then design a range of Map and Reduce tasks for each worker to pick up.

	for i, file := range files {
		c.mTasks = append(c.mTasks, Task{
			FilePath:   file,
			WorkId:     i,
			WorkType:   1,
			Status:     0,
			SlowLinked: nil,
		})
	}
	for i := range nReduce {
		c.rTasks = append(c.rTasks, Task{
			WorkId:       i,
			WorkType:     2,
			ReduceBucket: i,
			Status:       0,
			SlowLinked:   nil,
		})
	}

	c.server()
	return &c
}
