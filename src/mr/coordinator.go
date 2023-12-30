package mr

import (
	"log"
	"sync"
	"time"
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

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()

	// register worker if not registered
	if c.workerStatus[args.ProcessID] == "" {
		c.workerStatus[args.ProcessID] = "idle"
	}

	mapJob := c.PickMapJob()
	if mapJob != nil {
		reply.MapJob = mapJob
		reply.Done = false
		c.workerStatus[args.ProcessID] = "busy"
		c.mu.Unlock()
		return nil
	}

	// checks if all map jobs are done
	if !c.AllMapJobDone() {
		reply.Done = false
		c.mu.Unlock()
		return nil
	}

	reduceJob := c.PickReduceJob()
	if reduceJob != nil {
		reply.ReduceJob = reduceJob
		reply.Done = false
		c.workerStatus[args.ProcessID] = "busy"
		c.mu.Unlock()
		return nil
	}

	c.mu.Unlock()
	reply.Done = c.Done()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AllMapJobDone() bool {
	done := true
	for _, v := range c.mapStatus {
		if v.Status != "completed" {
			done = false
			break
		}
	}
	return done
}

func (c *Coordinator) ReportMapTask(args *ReportMapTaskArgs, reply *RequestJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapStatus[args.Filename] = JobStatus{StartTime: -1, Status: "completed"}
	c.workerStatus[args.ProcessID] = "idle"
	for r := 0; r < c.nReduce; r++ {
		c.intermediateFiles[r] = append(c.intermediateFiles[r], args.IntermediateFiles[r])
	}

	return nil
}

func (c *Coordinator) ReportReduceTask(args ReportReduceJobArgs, replyReduce *RequestJobReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceStatus[args.ReduceNumber] = JobStatus{StartTime: -1, Status: "completed"}
	c.workerStatus[args.ProcessID] = "idle"
	return nil
}

func (c *Coordinator) PickMapJob() *MapJob {
	var job *MapJob = nil
	for k, v := range c.mapStatus {
		if v.Status == "pending" {
			job = &MapJob{}
			job.FileName = k
			job.MapJobID = c.mapTaskID
			job.ReduceN = c.nReduce
			c.mapStatus[k] = JobStatus{StartTime: time.Now().Unix(), Status: "running"}
			c.mapTaskID++
			break
		}
	}

	return job
}

func (c *Coordinator) PickReduceJob() *ReduceJob {
	var job *ReduceJob = nil
	reducer := -1
	for i, v := range c.reduceStatus {
		if v.Status == "pending" {
			reducer = i
			break
		}
	}

	if reducer < 0 {
		return nil
	}

	job = &ReduceJob{}
	job.ReduceJobID = reducer
	job.IntermediateFiles = c.intermediateFiles[reducer]

	c.reduceStatus[reducer] = JobStatus{StartTime: time.Now().Unix(), Status: "running"}

	return job
}
func (c *Coordinator) StartTicker() {
	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					return
				}
				c.CheckDeadWorker()
			}
		}
	}()
}

func (c *Coordinator) CheckDeadWorker() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.mapStatus {
		if v.Status == "running" {
			now := time.Now().Unix()
			if v.StartTime > 0 && now > (v.StartTime+10) {
				c.mapStatus[k] = JobStatus{StartTime: -1, Status: "pending"}
				continue
			}
		}
	}
	for k, v := range c.reduceStatus {
		if v.Status == "running" {
			now := time.Now().Unix()
			if v.StartTime > 0 && now > (v.StartTime+10) {
				c.reduceStatus[k] = JobStatus{StartTime: -1, Status: "pending"}
				continue
			}
		}
	}
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
	ret := true

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, v := range c.reduceStatus {
		if v.Status != "completed" {
			return false
		}
	}
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
