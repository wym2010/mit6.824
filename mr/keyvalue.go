package mr

import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}


// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
var Nreduce = 0

// for sorting by key.
type ByBucket []KeyValue

func SetNreduce(v int)  {
	Nreduce = v
}
// for sorting by key.
func (a ByBucket) Len() int           { return len(a) }
func (a ByBucket) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByBucket) Less(i, j int) bool { return ihash(a[i].Key) % Nreduce < ihash(a[j].Key) % Nreduce }

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key  < a[j].Key  }

