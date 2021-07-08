package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type MapStatus struct {
	Input_file string
	Timestamp  time.Time
	Task_id    int
	Machine_id uuid.UUID
}

type ReduceStatus struct {
	Input_files []string
	Timestamp   time.Time
	Task_id     int
	Machine_id  uuid.UUID
}

type Coordinator struct {
	// tracking map tasks
	Idle_map_pool  chan MapStatus
	InProgress_map map[int]MapStatus
	Finished_map   map[int]MapStatus

	// tracking reduce tasks
	Idle_reduce_pool  chan ReduceStatus
	InProgress_reduce map[int]ReduceStatus
	Finished_reduce   map[int]ReduceStatus

	// Number of reduce jobs.
	Partition int

	// Number of map jobs, each map job produces
	// Partition intermediate outputs.
	Input_file_num int

	// lock
	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) AssignTask(args *RpcArgs, reply *RpcReply) error {
	c.lock.Lock()
	if len(c.Finished_reduce) == c.Partition {
		fmt.Println("finished all tasks")
		reply.Task_id = -1
		return nil
	}
	if args.Status == DONE {
		// fmt.Println("worker is done")
		if args.Is_map {
			status, exist := c.InProgress_map[args.Task_id]
			if exist && status.Machine_id == args.Machine_id {
				c.Finished_map[args.Task_id] = c.InProgress_map[args.Task_id]
				delete(c.InProgress_map, args.Task_id)
				if len(c.Finished_map) == c.Input_file_num {
					fmt.Println("finished all map tasks")
					for i := 0; i < c.Partition; i++ {
						reduce_status := ReduceStatus{
							Task_id: i,
						}
						for j := 0; j < c.Input_file_num; j++ {
							reduce_status.Input_files = append(reduce_status.Input_files, fmt.Sprintf("mr-%d-%d.gob", j, i))
						}
						c.Idle_reduce_pool <- reduce_status
					}
				}
			}
		} else {
			status, exist := c.InProgress_reduce[args.Task_id]
			if exist && status.Machine_id == args.Machine_id {
				c.Finished_reduce[args.Task_id] = c.InProgress_reduce[args.Task_id]
				delete(c.InProgress_reduce, args.Task_id)
			}
		}
		// fmt.Printf("current status:\n idle_map_task:%d \n idle_reduce_task:%d \n in_progress_map_task:%d \n in_progress_reduce_task:%d \n done_map_task:%d \n done_reduce_task:%d\n",
		// 	len(c.Idle_map_pool),
		// 	len(c.Idle_reduce_pool),
		// 	len(c.InProgress_map),
		// 	len(c.InProgress_reduce),
		// 	len(c.Finished_map),
		// 	len(c.Finished_reduce))
	}
	c.lock.Unlock()
	select {
	case status := <-c.Idle_map_pool:
		// fmt.Println("assign map task")
		reply.Input_files = append(reply.Input_files, status.Input_file)
		reply.Is_map = true
		reply.Task_id = status.Task_id
		reply.Partition = c.Partition
		c.lock.Lock()
		c.InProgress_map[status.Task_id] = MapStatus{
			Input_file: status.Input_file,
			Timestamp:  time.Now(),
			Task_id:    status.Task_id,
			Machine_id: args.Machine_id,
		}
		c.lock.Unlock()
	case status := <-c.Idle_reduce_pool:
		// fmt.Println("assign reduce task")
		reply.Input_files = status.Input_files
		reply.Is_map = false
		reply.Task_id = status.Task_id
		c.lock.Lock()
		c.InProgress_reduce[status.Task_id] = ReduceStatus{
			Input_files: status.Input_files,
			Timestamp:   time.Now(),
			Task_id:     status.Task_id,
			Machine_id:  args.Machine_id,
		}
		c.lock.Unlock()
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.lock.Lock()
	if len(c.Finished_reduce) == c.Partition {
		ret = true
	}

	c.lock.Unlock()
	return ret
}

func (c *Coordinator) scan() {
	for {
		time.Sleep(10 * time.Millisecond)
		c.lock.Lock()
		for id, status := range c.InProgress_map {
			curr := time.Now()
			diff := curr.Sub(status.Timestamp)
			if diff >= 10*time.Second {
				// fmt.Println("detected an obsolete worker")
				c.Idle_map_pool <- MapStatus{
					Task_id:    id,
					Input_file: c.InProgress_map[id].Input_file,
				}
				delete(c.InProgress_map, id)
				// fmt.Printf("current status:\n idle_map_task:%d \n idle_reduce_task:%d \n in_progress_map_task:%d \n in_progress_reduce_task:%d \n done_map_task:%d \n done_reduce_task:%d\n",
				// 	len(c.Idle_map_pool),
				// 	len(c.Idle_reduce_pool),
				// 	len(c.InProgress_map),
				// 	len(c.InProgress_reduce),
				// 	len(c.Finished_map),
				// 	len(c.Finished_reduce))
			}
		}
		for id, status := range c.InProgress_reduce {
			curr := time.Now()
			diff := curr.Sub(status.Timestamp)
			if diff >= 10*time.Second {
				// fmt.Println("detected an obsolete worker")
				c.Idle_reduce_pool <- ReduceStatus{
					Task_id:     id,
					Input_files: c.InProgress_reduce[id].Input_files,
				}
				delete(c.InProgress_reduce, id)
				// fmt.Printf("current status:\n idle_map_task:%d \n idle_reduce_task:%d \n in_progress_map_task:%d \n in_progress_reduce_task:%d \n done_map_task:%d \n done_reduce_task:%d\n",
				// 	len(c.Idle_map_pool),
				// 	len(c.Idle_reduce_pool),
				// 	len(c.InProgress_map),
				// 	len(c.InProgress_reduce),
				// 	len(c.Finished_map),
				// 	len(c.Finished_reduce))
			}
		}
		c.lock.Unlock()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Partition:      nReduce,
		Input_file_num: len(files),

		Idle_map_pool:  make(chan MapStatus, len(files)),
		InProgress_map: make(map[int]MapStatus),
		Finished_map:   make(map[int]MapStatus),

		Idle_reduce_pool:  make(chan ReduceStatus, nReduce),
		InProgress_reduce: make(map[int]ReduceStatus),
		Finished_reduce:   make(map[int]ReduceStatus),
	}
	for i, file := range files {
		fmt.Printf("add idle task %d input file: %v\n", i, file)
		c.Idle_map_pool <- MapStatus{
			Task_id:    i,
			Input_file: file,
		}
	}
	c.server()
	go c.scan()
	return &c
}
