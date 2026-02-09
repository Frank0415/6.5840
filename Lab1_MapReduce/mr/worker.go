package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type currentWork struct {
	inPath       string
	outPath      []string
	WorkId       int
	ReduceBucket int
	WorkType     int // 1 -> Map Work, 2 -> Reduce Work
}

type workerStat struct {
	workStatus  int // 0 -> No Work, 1 -> Mapper, 2 -> Reducer
	workerId    int
	totalMap    int
	totalReduce int
	currWork    currentWork
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	status, err := CallInitWorker()
	if err != nil {
		log.Fatalf("Failed to initialize worker: %v", err)
	}

	go CallCheckHealth(status.workerId)

	for {
		reply, err := CallGetWork(status.workerId)
		if err != nil {
			log.Fatalf("Failed to get work: %v", err)
		}

		status.workStatus = reply.WorkType
		status.currWork.WorkType = reply.WorkType

		status.currWork.WorkId = reply.WorkId
		status.currWork.ReduceBucket = reply.ReduceBucket
		status.currWork.inPath = reply.Path

		switch status.workStatus {
		case 0:
			// No Work
			time.Sleep(time.Second)
			continue
		case 1:
			// Map Work
			status.mapFunc(mapf)
		case 2:
			// Reduce Work
			status.reduceFunc(reducef)
		}

	}
}

func CallInitWorker() (workerStat, error) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		log.Printf("Worker registered with WorkerId %v\n", reply.WorkerId)
		return workerStat{
			workStatus:  0,
			workerId:    reply.WorkerId,
			totalMap:    reply.TotalMap,
			totalReduce: reply.TotalReduce,
		}, nil
	} else {
		log.Printf("Worker registration failed!\n")
		return workerStat{}, fmt.Errorf("call failed")
	}
}

// function to get work from coordinator and update worker status accordingly
// 0 -> No Work, 1 -> Map Work, 2 -> Reduce Work
// the RPC argument and reply types are defined in rpc.go.
func CallGetWork(WId int) (GetWorkReply, error) {
	args := GetWorkArgs{WorkerId: WId}
	reply := GetWorkReply{}

	ok := call("Coordinator.GetWork", &args, &reply)
	if ok {
		log.Printf("Worker %v received work: WorkType %v, WorkId %v\n", WId, reply.WorkType, reply.WorkId)
		return reply, nil
	} else {
		log.Printf("Worker %v failed to receive work\n", WId)
		return GetWorkReply{}, fmt.Errorf("call failed")
	}
}

func (w *workerStat) CallFinishWork(workType int) {
	args := FinishWorkArgs{}
	args.WorkerId = w.workerId
	args.WorkId = w.currWork.WorkId
	args.WorkType = workType
	reply := FinishWorkReply{}
	log.Printf("Worker %v finished work: WorkType %v, WorkId %v\n", w.workerId, workType, w.currWork.WorkId)
	call("Coordinator.FinishWork", &args, &reply)
}

func CallCheckHealth(WId int) {
	for {
		time.Sleep(1 * time.Second)
		args := CheckHealthArgs{WorkerId: WId}
		reply := CheckHealthReply{}

		ok := call("Coordinator.CheckHealth", &args, &reply)
		if ok {
			log.Printf("Worker %v heartbeat sent\n", WId)
		} else {
			log.Printf("Worker %v heartbeat failed!\n", WId)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (w *workerStat) mapFunc(mapf func(string, string) []KeyValue) {
	inFile, err := os.Open(w.currWork.inPath)
	if err != nil {
		log.Fatalf("cannot open %v", w.currWork.inPath)
	}
	content, err := ioutil.ReadAll(inFile)
	if err != nil {
		log.Fatalf("cannot read %v", w.currWork.inPath)
	}
	inFile.Close()

	kva := mapf(w.currWork.inPath, string(content))

	// Create temp files for intermediate output
	tempFiles := []*os.File{}
	tempFileNames := []string{}
	for i := 0; i < w.totalReduce; i++ {
		tempFile, err := os.CreateTemp(".", "mr-map-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		tempFiles = append(tempFiles, tempFile)
		tempFileNames = append(tempFileNames, tempFile.Name())
	}

	for _, kv := range kva {
		bucket := ihash(kv.Key) % w.totalReduce
		fmt.Fprintf(tempFiles[bucket], "%v %v\n", kv.Key, kv.Value)
	}

	for i, f := range tempFiles {
		f.Close()
		finalName := fmt.Sprintf("mr-%d-%d", w.currWork.WorkId, i)
		err := os.Rename(tempFileNames[i], finalName)
		if err != nil {
			log.Fatalf("cannot rename %v to %v: %v", tempFileNames[i], finalName, err)
		}
	}

	w.CallFinishWork(1)
}

func (w *workerStat) reduceFunc(reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	var ReadCount []bool
	var ReadDone int
	for range w.totalMap {
		ReadCount = append(ReadCount, false)
	}
	// infinite loop to wait for all map outputs to be available
	for {
		for i := range w.totalMap {
			if ReadCount[i] {
				continue
			}
			filename := fmt.Sprintf("mr-%d-%d", i, w.currWork.ReduceBucket)
			file, err := os.Open(filename)
			if err != nil {
				continue // Some maps might have failed or files missing?
				// In a real system we'd check if this is an error we can recover from.
			}

			// Reading formatted output back
			for {
				var kv KeyValue
				n, err := fmt.Fscanf(file, "%v %v\n", &kv.Key, &kv.Value)
				if n < 2 || err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			ReadCount[i] = true
			ReadDone++
			file.Close()
		}
		if ReadDone == w.totalMap {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", w.currWork.ReduceBucket)
	tempFile, err := os.CreateTemp(".", "mr-reduce-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	err = os.Rename(tempFile.Name(), oname)
	if err != nil {
		log.Fatalf("cannot rename %v to %v: %v", tempFile.Name(), oname, err)
	}

	w.CallFinishWork(2)
}
