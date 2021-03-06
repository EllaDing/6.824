package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// read input file,
// pass it to Map,
// return the intermediate Map output.
func read_from_file(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func get_map_results(filename string,
	mapf func(string, string) []KeyValue) []KeyValue {
	return mapf(filename, read_from_file(filename))
}

func reduce_and_write(key_vals []KeyValue, filename string,
	reducef func(string, []string) string) {
	tmpfile, _ := ioutil.TempFile("", "")
	defer os.Remove(tmpfile.Name()) // clean up
	sort.Sort(ByKey(key_vals))
	i := 0
	for i < len(key_vals) {
		j := i + 1
		for j < len(key_vals) && key_vals[j].Key == key_vals[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, key_vals[k].Value)
		}
		output := reducef(key_vals[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", key_vals[i].Key, output)
		i = j
	}
	os.Rename(tmpfile.Name(), filename)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	id := uuid.New()
	var is_map bool
	var task_id int
	status := IDLE
	for {
		reply, success := CallCoordinator(id, Status(status), task_id, is_map)
		// if Task_id == -1, all tasks are finished
		if !success || reply.Task_id == -1 {
			break
		}
		if len(reply.Input_files) == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Is_map {
			results := make(map[int][]KeyValue)
			for _, file := range reply.Input_files {
				key_vals := get_map_results(file, mapf)
				for _, key_val := range key_vals {
					number := ihash(key_val.Key) % reply.Partition
					results[number] = append(results[number], key_val)
				}
			}
			//  For each partition, write the reduced result to a file.
			for partition, val := range results {
				filename := fmt.Sprintf("mr-%d-%d.gob", reply.Task_id, partition)
				tmpfile, _ := ioutil.TempFile("", "")
				defer os.Remove(tmpfile.Name()) // clean up
				enc := gob.NewEncoder(tmpfile)
				err := enc.Encode(val)
				if err != nil {
					log.Fatal("encode:", err)
				}
				e := os.Rename(tmpfile.Name(), filename)
				if e != nil {
					log.Fatal(e)
				}
			}
		} else {
			var kvs []KeyValue
			for _, file := range reply.Input_files {
				var data []KeyValue
				dataFile, err := os.Open(file)
				if err != nil {
					if os.IsNotExist(err) {
						// It's normal that the map job doesn't produced results for this specific partition.
						continue
					}
					log.Fatal("decode:", err)
				}

				dataDecoder := gob.NewDecoder(dataFile)
				err = dataDecoder.Decode(&data)
				for _, d := range data {
					kvs = append(kvs, d)
				}
				dataFile.Close()
			}
			ofile_reduce := fmt.Sprintf("mr-out-%d", reply.Task_id)
			reduce_and_write(kvs, ofile_reduce, reducef)
		}
		status = DONE
		task_id = reply.Task_id
		is_map = reply.Is_map
	}
}

// the RPC argument and reply types are defined in rpc.go.

func CallCoordinator(id uuid.UUID, status Status, task_id int, is_map bool) (RpcReply, bool) {

	// declare an argument structure.
	args := RpcArgs{}
	args.Status = status
	args.Machine_id = id
	args.Task_id = task_id
	args.Is_map = is_map

	// declare a reply structure.
	reply := RpcReply{}

	// send the RPC request, wait for the reply.
	success := call("Coordinator.AssignTask", &args, &reply)
	return reply, success
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
