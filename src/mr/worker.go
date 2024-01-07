package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
	shouldExit := false
	for !shouldExit {
		reply := RequestTask()

		if reply.Done {
			shouldExit = true
			// fmt.Println("All Jobs Completed")
			continue
		}

		if reply.MapJob != nil {
			handleMapTask(reply.MapJob, mapf)
		}

		if reply.ReduceJob != nil {
			handleReduceTask(reply.ReduceJob, reducef)
		}
		// time.Sleep(1 * time.Second)
	}
}

func handleMapTask(job *MapJob, mapf func(string, string) []KeyValue) {
	fileName := job.FileName
	reduceCount := job.ReduceN
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	sort.Sort(ByKey(kva))
	partitionedKva := make([][]KeyValue, reduceCount)
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % reduceCount
		partitionedKva[reduceIndex] = append(partitionedKva[reduceIndex], kv)
	}

	intermediateFiles := make([]string, reduceCount)
	for i := 0; i < reduceCount; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", job.MapJobID, i) // generate file name, including the mapjob id and reduce identifier
		intermediateFiles[i] = intermediateFileName
		intermediateFile, err := os.Create(intermediateFileName)

		if err != nil {
			log.Fatalf("cannot create %v", intermediateFileName)
		}
		data, err := json.Marshal(partitionedKva[i])
		if err != nil {
			fmt.Println("Error marshalling data: ", err)
		}
		intermediateFile.Write(data)
		intermediateFile.Close()
	}

	ReportMapTask(ReportMapTaskArgs{Filename: fileName, IntermediateFiles: intermediateFiles, ProcessID: os.Getpid()})

}

func handleReduceTask(job *ReduceJob, reducef func(string, []string) string) {
	files := job.IntermediateFiles
	intermediate := []KeyValue{}

	for _, fileName := range files {
		data, err := ioutil.ReadFile(fileName)
		if err != nil {
			fmt.Println("Read error: ", err.Error())
		}
		var input []KeyValue
		err = json.Unmarshal(data, &input) // converts json to Go structs and places the result in the value pointed to by &input
		if err != nil {
			fmt.Println("Unmarshal error: ", err.Error())
		}

		intermediate = append(intermediate, input...)
	}

	sort.Sort(ByKey(intermediate))

	// generating output file
	outputName := fmt.Sprintf("mr-out-%v", job.ReduceJobID)
	tempFile, err := ioutil.TempFile(".", outputName)
	if err != nil {
		fmt.Println("Error creating temp file")
	}

	i := 0
	// taken from mrsequential.go
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	os.Rename(tempFile.Name(), outputName)
	ReportReduceTask(ReportReduceJobArgs{ProcessID: os.Getpid(), ReduceNumber: job.ReduceJobID})
}

func ReportMapTask(args ReportMapTaskArgs) ReportMapTaskReply {
	reply := ReportMapTaskReply{}
	call("Coordinator.ReportMapTask", &args, &reply)
	return reply
}

func ReportReduceTask(args ReportReduceJobArgs) ReportReduceTaskReply {
	reply := ReportReduceTaskReply{}
	call("Coordinator.ReportReduceTask", &args, &reply)
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func RequestTask() RequestTaskReply {

	// declare an argument structure.
	args := RequestTaskArgs{}

	// fill in the argument(s).
	args.ProcessID = os.Getpid()

	// declare a reply structure.
	reply := RequestTaskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.RequestTask", &args, &reply)

	// reply.Y should be 100.
	// fmt.Printf("reply.Y %v\n", reply.Y)
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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
