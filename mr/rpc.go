package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

//
type RegisterArgs struct {
	Sockname string
}

type RegisterReply struct {
	WorkerId int
	NReduce  int
}

type RequestArgs struct {
	WorkerId int
}

type RequestReply struct {
	// Map or Reduce
	Type       string
	MapTask    MapTask
	ReduceTask ReduceTask
}

type TaskCompletedArgs struct {
	Id int
	//Map or Reduce
	Type string
}

type TaskCompletedReply struct {
}

type Args struct {
	Action string
	// assign,
}

type Reply struct {
	KeyValue
	Id int
	// Map :1,  Reduce: 2
	Type    int
	Nreduce int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
