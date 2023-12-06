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

type JobType int

const (
	MapJob    JobType = 1
	ReduceJob JobType = 2
	WaitJob   JobType = 3
	EndJob    JobType = 4
)

type HeartBeatArgs struct {
	// state == -1: request for new job; else report finish job with id == state
	State int
}

type HeartBeatReply struct {
	Id       int
	JobType  JobType
	FileName string
	NWorker  int // 在Map阶段表示有多少reduce，在Reduce阶段表示有多少Map
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
