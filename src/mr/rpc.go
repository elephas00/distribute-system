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

// Add your RPC definitions here.
type WorkerDiscoverArgs struct {

}

type WorkerDiscoverReply struct {
	WorkerId int32
}

type HeartBeatArgs struct {
	WorkerId int32
}

type HeartBeatReply struct {
	Success bool
}

type RequestTaskArgs struct {
	WorkerId int32
}

type RequestTaskReply struct {
	Success bool
	Task Task
}

type TaskDoneArgs struct {
	Task Task
}

type TaskDoneRelpy struct {
	Success bool
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
