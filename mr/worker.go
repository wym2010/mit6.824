package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"

	"github.com/wonderivan/logger"
)

type WorkerInfo struct {
	Id int
	//Map or Reduce
	WorkerType string
	Nreduce    int
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	wi := WorkerInfo{
		Id:         0,
		WorkerType: "",
		Nreduce:    0,
	}
	// Register this node to master
	wi.Register()

	// Request tasks in an infinite loop
	for {
		ra := RequestArgs{
			WorkerId: wi.Id,
		}
		rr := RequestReply{}

		Request(&ra, &rr)

		if rr.Type == "Map" {
			kva := mapf(rr.MapTask.key, rr.MapTask.value)

			WriteToIntermedia(kva, wi.Nreduce, wi.Id)

			TaskCompleted(wi)

		} else if rr.Type == "Reduce" {

			if 1 == useInternalSort(rr.ReduceTask.Files) {
				kva := []KeyValue{}
				Aggregrate(kva, rr.ReduceTask.Files)
				sort.Sort(ByKey(kva))

				DoReduce(kva, rr.ReduceTask.id, reducef)

			} else {
				//TODO
			}

		} else if rr.Type == "Finished" {
			os.Exit(0)
		}
	}
}

// Write the key value array into mr-X-Y files,
// which is a intermediate file as input in "reduce phase"
func WriteToIntermedia(kva []KeyValue, Nreduce int, MapTaskNum int) {
	// initialization
	kvaForEachNReduce := make([][]KeyValue, Nreduce)
	for i := range kvaForEachNReduce {
		kvaForEachNReduce[i] = make([]KeyValue, 1)
	}

	// Fill the intermediate kv array
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % Nreduce
		kvaForEachNReduce[reduceTaskNum] = append(kvaForEachNReduce[reduceTaskNum], kv)
	}

	//for each reduceTaskNum,
	// write kv array to local disk
	for reduceTaskNum := range kvaForEachNReduce {
		tempFileName := fmt.Sprintf("mr-%d-%d", MapTaskNum, reduceTaskNum)
		tempFile, err := ioutil.TempFile("", tempFileName)
		tempFileFullName := tempFile.Name()

		enc := json.NewEncoder(tempFile)
		enc.Encode(kvaForEachNReduce[reduceTaskNum])
		// To ensure that nobody observes partially written files in the presence of crashes,
		//the MapReduce paper mentions the trick of using a temporary file and atomically renaming it
		//once it is completely written.
		err = os.Rename(tempFileFullName, tempFileName)
		if err != nil {
			logger.Error(err)
			return
		}
	}

}

//Aggregrate kva from intermediate files
func Aggregrate(kva []KeyValue, files []string) {
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			logger.Error("in func Aggregrate open file failed")
			return
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

	}
}

// Run reduce function and generate final output
func DoReduce(intermediate []KeyValue, reduceid int, reducef func(string, []string) string) {

	oname := "mr-out-"
	oname = oname + strconv.Itoa(reduceid)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

func useInternalSort(files []string) int {
	return 1
}

func Request(a *RequestArgs, r *RequestReply) {
	call("Master.Request", a, r)
	logger.Debug(a, r)

}

func TaskCompleted(wi WorkerInfo) {
	ta := TaskCompletedArgs{
		Id:   wi.Id,
		Type: wi.WorkerType,
	}
	tr := TaskCompletedReply{}

	call("Master.TaskCompleted", &ta, &tr)

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//Register current worker to master

func (ri *WorkerInfo) Register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	isSuccess := call("Master.Register", &args, &reply)

	//Exit if it fails
	if !isSuccess {
		fmt.Println("registration failed")
		os.Exit(1)
	}
	// Register this node to master Master will reply registration with a Id and Nreduce
	ri.Nreduce = reply.NReduce
	ri.Id = reply.WorkerId
	fmt.Println("registration success")

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
