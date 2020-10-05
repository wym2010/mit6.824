package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu *sync.Mutex

type MapTask struct {
	id       int
	filename string
	status   int
	// 1: idle, 2: complete, 3: in progress
	starttime int
}

type ReduceTask struct {
	id     int
	status int
	// 1: idle, 2: complete, 3: in progress
}
type Master struct {
	// Your definitions here.
	maptasks      []MapTask
	reducetasks   []ReduceTask
	nReduce       int
	CurrentMap    int
	CurrentReduce int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) Request(args *Args, reply *Reply) error {
	fmt.Printf("in request\n")
	defer fmt.Printf("out request\n")

	mu.Lock()
	defer mu.Unlock()
	if !m.mapAllFinished() {
		for i, maptask := range m.maptasks {
			// unassigned maptask found
			if maptask.status == 1 {
				reply.KeyValue = KeyValue{Key: maptask.filename, Value: ""}
				// mark type as a map task
				reply.Type = 1
				reply.Id = m.CurrentMap
				reply.Nreduce = m.nReduce
				fmt.Printf("assign maptask (%d), nreduce: (%d), filename: (%s)\n", reply.Id, reply.Nreduce, reply.Key)
				m.CurrentMap += 1
				m.maptasks[i].status = 3
				go func(i int) {

					timer := time.NewTimer(time.Second * 10)
					<-timer.C
					mu.Lock()
					defer mu.Unlock()
					if m.maptasks[i].status != 2 {
						m.maptasks[i].status = 1
					}
				}(i)
				return nil
			}
		}
	} else if !m.ReduceAllFinished() {
		for i, reducetask := range m.reducetasks {
			// unassigned reduce task found
			if reducetask.status == 1 {
				reply.Type = 2
				m.reducetasks[i].status = 3
				reply.Id = m.CurrentReduce
				fmt.Printf("assign reduce task (%d), \n", reply.Id)
				m.CurrentReduce += 1
				go func(i int) {
					timer := time.NewTimer(time.Second * 10)
					<-timer.C
					mu.Lock()
					defer mu.Unlock()
					// after 10 seconds not fnished
					if m.reducetasks[i].status != 2 {
						m.reducetasks[i].status = 1
					}
				}(i)
				return nil
			}
		}
	}
	return nil
}

func (m *Master) Finished(args *FinishedArgs, reply *FinishedReply) error {
	fmt.Printf("in Finished\n")
	defer fmt.Printf("out Finished\n")

	mu.Lock()
	defer mu.Unlock()

	if args.Type == 1 {
		for i, maptask := range m.maptasks {

			maptaskid := maptask.id
			if maptaskid == args.Id {
				if args.Result {
					m.maptasks[i].status = 2
				} else {
					m.maptasks[i].status = 1
				}
				// when map task finished, create reduce tasks
				if m.mapAllFinished() {
					index := 0
					for ; index < m.nReduce; index++ {
						reducetask := ReduceTask{id: index, status: 1}
						m.reducetasks = append(m.reducetasks, reducetask)
					}
				}

				return nil
			}
		}

		fmt.Printf("id : %d not found for map work", args.Id)
	} else if args.Type == 2 {
		for i, reducetask := range m.reducetasks {
			if reducetask.id == args.Id {
				if args.Result {
					m.reducetasks[i].status = 2
				} else {
					m.reducetasks[i].status = 1
				}

				return nil
			}
		}
		fmt.Printf("id : %d not found for reduce work", args.Id)
	} else {
		fmt.Printf("unexpected type\n")
	}
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if m.mapAllFinished() && m.ReduceAllFinished() {
		return true

	} else {
		return false
	} // Your code here.
}

func (m *Master) mapAllFinished() bool {
	maptasks := m.maptasks
	for _, maptask := range maptasks {
		if maptask.status != 2 {
			return false
		}
	}
	return true
}
func (m *Master) ReduceAllFinished() bool {
	mu.Lock()
	reducetasks := m.reducetasks
	mu.Unlock()
	for _, reducetask := range reducetasks {
		if reducetask.status != 2 {
			return false
		}
	}
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// Nreduce is the number of reduce tasks to use.
//

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nReduce: nReduce}
	mu = new(sync.Mutex)
	// Your code here.
	mu.Lock()
	defer mu.Unlock()

	m.InitMapTasks(files)

	m.server()
	return &m
}

func (m *Master) InitMapTasks(files []string) {
	for index, filename := range files {
		maptask := MapTask{filename: filename, status: 1, id: index}
		m.maptasks = append(m.maptasks, maptask)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	log.Println("server: rpc")
	reply.Y = args.X + 1
	return nil
}
