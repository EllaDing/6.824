package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"

	"github.com/google/uuid"
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

type Status string

const (
	IDLE = "idle"
	DONE = "completed"
)

type RpcArgs struct {
	Status     Status
	Machine_id uuid.UUID
	Is_map     bool
	Task_id    int
}

type RpcReply struct {
	Input_files []string
	Partition   int
	Is_map      bool
	Task_id     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
