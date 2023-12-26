package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type JobStatus struct {
	StartTime int64
	Status    string
}

type Coordinator struct {
	// Your definitions here.
	workerStatus      map[int]string
	mapStatus         map[string]JobStatus
	reduceStatus      map[int]JobStatus
	mapTaskID         int
	nReduce           int
	intermediateFiles map[int][]string
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask() {

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) StartTicker() {

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
	c := Coordinator{}

	// Your code here.
	c.mapTaskID = 0
	c.mapStatus = make(map[string]JobStatus)
	for _, v := range files {
		c.mapStatus[v] = JobStatus{StartTime: -1, Status: "pending"}
	}

	c.nReduce = nReduce
	c.reduceStatus = make(map[int]JobStatus)
	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = JobStatus{StartTime: -1, Status: "pending"}
	}
	c.workerStatus = make(map[int]string)
	c.intermediateFiles = make(map[int][]string)
	c.StartTicker()
	c.server()
	return &c
}
