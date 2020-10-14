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

	"github.com/wonderivan/logger"
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

//

// RPC handlers for the worker to call.

func (m *Master) Request(args *RequestArgs, reply *RequestReply) error {

	mu.Lock()
	defer mu.Unlock()
	for i, maptask := range m.mapTasks {
		// unassigned maptask found
		if maptask.status == IDEL {
			reply.MapTask = maptask
			reply.Type = "Map"
			logger.Debug("assign maptask : ", maptask, " to: ", args.WorkerId)
			m.mapTasks[i].status = INPROGRESS

			return nil
		}
	}
	for i, reducetask := range m.reduceTasks {
		// unassigned maptask found
		if reducetask.status == IDEL {
			reply.ReduceTask = reducetask
			reply.Type = "Reduce"
			logger.Debug("assign redude : ", reducetask, " to: ", args.WorkerId)
			m.reduceTasks[i].status = INPROGRESS

			return nil
		}
	}
	reply.Type = "Finished"
	logger.Debug("assign Finished", " to: ", args.WorkerId)
	return nil
}

//end of  RPC handlers for the worker to call.

func (m *Master) TaskCompleted(ta *TaskCompletedArgs, tr *TaskCompletedReply) error {

	mu.Lock()
	defer mu.Unlock()

	if ta.Type == "Map" {
		for i, maptask := range m.mapTasks {

			if maptask.id == ta.Id && maptask.status == INPROGRESS {
				m.mapTasks[i].status = COMPLETE
				// when map task finished, create reduce tasks
				isFinished := m.mapAllFinished()
				if isFinished {
					m.initReduceTasks()
				}

				return nil
			}
		}

	} else if ta.Type == "Reduce" {
		for i, reducetask := range m.reduceTasks {

			if reducetask.id == ta.Id && reducetask.status == INPROGRESS {
				m.reduceTasks[i].status = COMPLETE
				// when map task finished, create reduce tasks

				return nil
			}
		}

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

// Must be called after fetching lock
func (m *Master) mapAllFinished() bool {
	for _, maptask := range m.mapTasks {
		if maptask.status != COMPLETE {
			return false
		}
	}
	return true
}

func (m *Master) ReduceAllFinished() bool {
	for _, reducetask := range m.reduceTasks {
		if reducetask.status != COMPLETE {
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

func (m *Master) initReduceTasks() {
	base := func() string {
		return "/root/go/src/labs/main/mrworker/"
	}

	for mapX := 1; mapX < len(m.mapTasks); mapX++ {
		files := make([]string, 0)
		for reduceY := 1; reduceY < m.nReduce; reduceY++ {
			file := fmt.Sprintf("mr-%d-%d", mapX, reduceY)
			file += base()
			files = append(files, file)
		}

		m.currentReduce += 1
		reducetask := ReduceTask{
			id:     m.currentReduce,
			Files:  files,
			status: IDEL,
		}
		m.reduceTasks = append(m.reduceTasks, reducetask)
	}
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapTasks:          nil,
		reduceTasks:       nil,
		RegisteredWorkers: nil,
		nReduce:           0,
	}
	mu.Lock()
	defer mu.Unlock()

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
		//Skip the file that failed to open
		if err != nil {
			logger.Error("an error occures when open file: ", filename)
		} else {
			bytes, err := ioutil.ReadAll(file)
			if err != nil {
				logger.Error("an error occures when readAll file: ", filename)
			} else {
				//Skip the file that failed to read

				// Increase map count
				m.currentMap += 1
				maptask := MapTask{
					id:        m.currentMap,
					key:       filename,
					value:     string(bytes),
					status:    1,
					starttime: time.Now(),
				}
				// Add the map task to master struct
				m.mapTasks = append(m.mapTasks, maptask)
			}
		}
		logger.Debug("mapTask is: ", m.mapTasks)
		logger.Debug("currentMap count is :", m.currentMap)

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
