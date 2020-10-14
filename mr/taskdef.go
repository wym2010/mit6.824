package mr

import "time"

const (
	IDEL       = 1
	COMPLETE   = 2
	INPROGRESS = 3
)

type MapTask struct {
	id     int
	key    string
	value  string
	status int
	// 1: idle, 2: complete, 3: in progress
	starttime time.Time
}

type ReduceTask struct {
	id int
	// Reduce intermediate file location
	Files []string

	// 1: idle, 2: complete, 3: in progress
	status int
}
