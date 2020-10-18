package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// worker host infomation  struct
type RegistedWorker struct {
	WorkerId int
	Sockname string
}

//main status struct to record task status, also keep worker host information list
type Master struct {
	// Your definitions here.
	mapTasks          []MapTask
	reduceTasks       []ReduceTask
	RegisteredWorkers []RegistedWorker
	nReduce           int
	currentMap        int
	currentReduce     int
	currentWorkerId   int
}

// the lock of operating the master struct
var mu = sync.Mutex{}

// This Cond is to monitor task status changing
//
var cond = sync.NewCond(&mu)

var base = func() string {

	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	} else {

		return home + "/go/src/labs/"
	}
}

//

// RPC handlers for the worker to call.

// client can call this function using RPC to request a new task
func (m *Master) Request(args *RequestArgs, reply *RequestReply) error {

	mu.Lock()
	defer mu.Unlock()

	inprogressFound := false

	// loop to find idel map task
	for i, maptask := range m.mapTasks {
		// unassigned maptask found
		switch maptask.Status {
		case IDEL:
			reply.MapTask = maptask
			reply.Type = "Map"
			reply.TaskId = maptask.Id
			m.mapTasks[i].Status = INPROGRESS
			log.Printf("worker #%d takes map task #%d", args.WorkerId, maptask.Id)
			return nil
		case INPROGRESS:
			inprogressFound = true
		case COMPLETE:

		default:
			log.Fatalln("Error Status: ", maptask.Status)
			os.Exit(2)
		}
	}

	// Loop to find idel reduce task
	for i, reducetask := range m.reduceTasks {
		// unassigned maptask found
		switch reducetask.Status {
		case IDEL:
			reply.ReduceTask = reducetask
			reply.Type = "Reduce"
			log.Println("assign redude : ", reply, " to: ", args.WorkerId)
			reply.TaskId = reducetask.Id
			m.reduceTasks[i].Status = INPROGRESS
			return nil
		case INPROGRESS:
			inprogressFound = true
		case COMPLETE:

		default:
			log.Fatalln("Error Status: ", reducetask.Status)
			os.Exit(2)
		}
	}
	// All completed
	if !inprogressFound {
		reply.Type = "Finished"
		return nil
	} else {
		// Some inprogress
		reply.Type = "Busy"
		return nil
	}
}

func (m *Master) TaskCompleted(ta *TaskCompletedArgs, tr *TaskCompletedReply) error {

	mu.Lock()
	defer mu.Unlock()

	switch ta.Type {
	case "Map":
		for i, maptask := range m.mapTasks {
			if maptask.Id == ta.TaskId && maptask.Status == INPROGRESS {
				m.mapTasks[i].Status = COMPLETE
				// when map task finished, create reduce tasks
				isFinished := m.mapAllFinished()
				if isFinished {
					m.initReduceTasks(base())
				}
				return nil
			}
		}
	case "Reduce":
		for i, reducetask := range m.reduceTasks {
			if reducetask.Id == ta.TaskId && reducetask.Status == INPROGRESS {
				m.reduceTasks[i].Status = COMPLETE
				return nil
			}
		}
	default:
		fmt.Printf("unexpected type\n")
	}
	return nil
}

//end of  RPC handlers for the worker to call.

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

// Must be called after fetching lock
func (m *Master) mapAllFinished() bool {
	for _, maptask := range m.mapTasks {
		if maptask.Status != COMPLETE {
			return false
		}
	}
	return true
}

func (m *Master) ReduceAllFinished() bool {
	for _, reducetask := range m.reduceTasks {
		if reducetask.Status != COMPLETE {
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

func (m *Master) initReduceTasks(base string) {

	for mapX := 1; mapX < len(m.mapTasks); mapX++ {
		files := make([]string, 0)
		for reduceY := 1; reduceY < m.nReduce; reduceY++ {
			file := fmt.Sprintf("mr-%d-%d", mapX, reduceY)
			file = base + file
			files = append(files, file)
		}

		m.currentReduce += 1
		reducetask := ReduceTask{
			Id:              m.currentReduce,
			IntermediaFiles: files,
			Status:          IDEL,
		}
		m.reduceTasks = append(m.reduceTasks, reducetask)
	}
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapTasks:          nil,
		reduceTasks:       nil,
		RegisteredWorkers: nil,
		nReduce:           nReduce,
	}

	m.initMapTasks(files)

	m.server()
	//m.ppathpathing()
	return &m
}

func (m *Master) initMapTasks(files []string) {
	mu.Lock()
	defer mu.Unlock()
	for _, filename := range files {
		file, err := os.Open(filename)
		//Skip the file that failed to open)
		if err != nil {
			log.Fatalln("an error occures when open file: ", filename)
		} else {
			bytes, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalln("an error occures when readAll file: ", filename)
			} else {
				//Skip the file that failed to read

				// Increase map count
				m.currentMap += 1
				maptask := MapTask{
					Id:        m.currentMap,
					Key:       filename,
					Value:     string(bytes),
					Status:    IDEL,
					Starttime: time.Now(),
				}
				// Add the map task to master struct
				m.mapTasks = append(m.mapTasks, maptask)
			}
		}
		log.Println("number of tasks is : ", len(m.mapTasks))

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
	log.Println("Listen in:", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// worker can call it with RPC to regiter itself
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	log.Println("register worker", args.Sockname)
	//Increase worker id
	m.currentWorkerId += 1

	newWorker := RegistedWorker{
		Sockname: args.Sockname,
		WorkerId: m.currentWorkerId,
	}
	// append newly registered workers
	m.RegisteredWorkers = append(m.RegisteredWorkers, newWorker)

	reply.WorkerId = m.currentWorkerId
	reply.NReduce = m.nReduce

	return nil
}
