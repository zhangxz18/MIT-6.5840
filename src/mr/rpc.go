package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// taskno: the coordinator has no task to dispatch
// taskfinish: all work is done and the coordinator tells the workers to exit
type TaskType int
const (
	// the zero value will be omitted in the rpc encoding, it's a better solution to add another value like TaskNohting for zero
	TaskNo TaskType=iota 
	TaskMap
	TaskReduce
	TaskFinish
)


// Add your RPC definitions here.
type AskForTaskArgs struct {
	FinishedTaskType TaskType
	FinishTaskList []int
	FinishedTaskFile []string
}

// dispatchedtasklist: the list of the map/reduce task index, which is corresponding to a file in the coordinator
type AskForTaskReply struct {
	DispatchedTaskType TaskType
	DispatchedTaskList []int
	DispatchedTaskFile []string
	ReduceNum int
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
