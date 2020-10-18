package mr

import "time"

const (
	IDEL       = 1
	COMPLETE   = 2
	INPROGRESS = 3
)

type MapTask struct {
	Id     int
	Key    string
	Value  string
	Status int
	// 1: idle, 2: complete, 3: in progress
	Starttime time.Time
}

type ReduceTask struct {
	Id int
	// Reduce intermediate file location
	IntermediaFiles []string

	// 1: idle, 2: complete, 3: in progress
	Status    int
	Starttime time.Time
}
