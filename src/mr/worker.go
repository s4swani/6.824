package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "math/rand"


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

//
// Returns a unique 64-bit int WorkerId
//
func GetId() WorkerId {
	
	x := rand.NewSource(time.Now().UnixNano())
	y := rand.New(x)

	return WorkerId(y.Intn(time.Now().Nanosecond()))
}

type WorkerInfo struct {

	MyWorkerId WorkerId

	TaskId int

	TaskType int

	TaskStatus int
	
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := GetId()

	CallAssignTask(workerId)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallAssignTask(workerId WorkerId) {

	// declare an argument structure.
	args := AssignTaskArgs{}

	// fill in the argument(s).
	args.CurrentWorkerId = workerId

	// declare a reply structure.
	reply := AssignTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.AssignTask" tells the
	// receiving server that we'd like to call
	// the AssignTask() method of struct Coordinator.
	ok := call("Coordinator.AssignTask", &args, &reply)
	
	if ok {
		fmt.Printf("Task Details %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
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
