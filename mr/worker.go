package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"

type WorkerInfo struct {
	Id         int
	workerType int
	// map :1 reduce: 2
	filename string
	Nreduce  int
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		args := Args{Action: "AssignWork"}
		reply := Reply{}
		Request(&args, &reply)
		if reply.Type != 0 {
			go func(reply Reply) {
				workerInfo := WorkerInfo{}
				InitWorkerInfo(reply, &workerInfo)
				result := DoWork(workerInfo, mapf, reducef)
				finishedargs := FinishedArgs{Id: reply.Id, Type: reply.Type, Result: result}
				finishedreply := FinishedReply{}
				call("Master.Finished", &finishedargs, &finishedreply)
			}(reply)
		} else {
			os.Exit(0)
		}
	}
}
func Request(pargs *Args, preply *Reply) {
	args := Args{}

	call("Master.Request", &args, preply)

	log.Printf("reply.type %v reply.nreduce %v reply.id %v\n", preply.Type, preply.Nreduce, preply.Id)
}
func InitWorkerInfo(reply Reply, w *WorkerInfo) {
	w.Id = reply.Id
	w.workerType = reply.Type
	w.Nreduce = reply.Nreduce
	if reply.Type == 1 {
		w.filename = reply.KeyValue.Key
	} else if reply.Type == 2 {

	}
}

func DoWork(w WorkerInfo, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	SetNreduce(w.Nreduce)

	if w.workerType == 1 {
		intermediate := []KeyValue{}
		filename := w.filename
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)

		sort.Sort(ByBucket(intermediate))
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && ihash(intermediate[j].Key)%Nreduce == ihash(intermediate[i].Key)%Nreduce {
				j++
			}
			reducegroup := []KeyValue{}
			GroupY := ihash(intermediate[i].Key) % w.Nreduce
			GroupX := w.Id
			for k := i; k < j; k++ {
				reducegroup = append(reducegroup, intermediate[k])
			}
			truefilename := "mr-" + strconv.Itoa(GroupX) + "-" + strconv.Itoa(GroupY)
			file, err = ioutil.TempFile(".", "tmp.json")
			defer file.Close()

			if err != nil {
				log.Fatal("%v\n", err)
				return false
			}
			filename := file.Name()
			enc := json.NewEncoder(file)
			err := enc.Encode(reducegroup)
			if err != nil {
				log.Fatal("Encode fail! %s", filename)
				return false
			} else {
				err = os.Rename(filename, truefilename)
				if err != nil {
					log.Fatal("%s aleady exist", truefilename)
				}
			}

			i = j
		}
		return true

	} else if w.workerType == 2 {
		matches, _ := filepath.Glob("./mr-*-" + strconv.Itoa(w.Id))
		intermediate := []KeyValue{}
		for _, filename := range matches {

			file, err := os.Open(filename)
			defer file.Close()
			if err != nil {
				log.Fatal("open file %s failed", filename)
				return false
			}
			dec := json.NewDecoder(file)
			kv := []KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				log.Fatal("failed decode \n")
				log.Fatal(err.Error() + "\n")
				return false
			}
			intermediate = append(intermediate, kv...)

		}
		sort.Sort(ByKey(intermediate))

		tempFile, err := ioutil.TempFile(".", "output.json")

		if err != nil {
			log.Fatal("create template %s failed", tempFile)
			return false
		}
		mrfilename := tempFile.Name()

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
			fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
			i = j
		}

		tempFile.Close()

		os.Rename(mrfilename, "mr-out-"+strconv.Itoa(w.Id))
		return true
	} else {
		log.Fatal("wrong type %v\n", w.workerType)
		return false
	}
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
