package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"runtime"
)

const (
	Unassigned = 0
	InProgress = 1
	Complete = 2
)



type Coordinator struct {
	
	//
	// MapTaskList: Full list of Map tasks and their
	// status maintained by the Coordinator.
	//
	MapTaskList map[string]int

	//
	// ReduceTaskList: Full list of all the Reduce Tasks and
	// their status maintained by the Coordinator
	//
	ReduceTaskList map[int]int

	//
	// AssignedMapTasks: Map tasks that are currently assigned
	// to a worker process
	//
	AssignedMapTasks map[string]*WorkerDetails

	//
	// AssignedReduceTasks: Reduce tasks that are currently
	// assigned to a worker process
	//
	AssignedReduceTasks map[int]WorkerId

	//
	// NumPendingMapTasks: number of map tasks pending completion
	//
	NumPendingMapTasks int

	//
	// NumPendingReduceTasks: number of reduce tasks pending completion
	//
	NumPendingReduceTasks int

	//
	// TaskIdCounter: current value gives us the Task Id
	//
	TaskIdCounter int

	//
	// MuLock: synchronise access to Coordinator struct
	//
	MuLock sync.Mutex
}

type WorkerDetails struct {

	workerId WorkerId
	workerNum int
	
}


func trace() {

	pc, _ , line, ok := runtime.Caller(1)

	fmt.Println()
	
	if !ok {
		fmt.Printf("%s:%d\n", "?", 0)
	}

	fn := runtime.FuncForPC(pc)
	
	if fn == nil {
		fmt.Printf("%s:%d\n", "?", 0)
	}

	fmt.Printf("%s():%d\n", fn.Name(), line)
}

func showPendingTasks(numMap int, numReduce int) {

	fmt.Printf("Number of map tasks pending: %d\n", numMap)

	fmt.Printf("Number of reduce tasks pending: %d\n", numReduce)
}

func (c *Coordinator) getUnassignedTask () (bool, string) {

	var unassigned = false
	var filename = ""
	var status int
	
	for filename, status = range c.MapTaskList {

		// we'll pick the first unassigned task we find
		if status == Unassigned {
			unassigned = true
			break
		}
	}

	return unassigned, filename
}

// Your code here -- RPC handlers for the worker to call.

//
// AssignTask: RPC called by the worker to ask for a task
//
// The Coordinator send a ReplyStatus of -1, 0, or 1 depending
// on whether all tasks are complete or pending
// 
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {

	fmt.Printf("Assign Task: WorkerId: %d\n", args.CurrentWorkerId)
	
	c.MuLock.Lock()

	if c.NumPendingMapTasks > 0 {

		// map tasks have not yet completed. assign map tasks

		unassignedTaskPresent, filename := c.getUnassignedTask()

		fmt.Printf("Unassgiend task present = %t, filename = %s\n", unassignedTaskPresent, filename)
		
		// Ask if there are unassigned map tasks
		if (unassignedTaskPresent) {

			// Update the coordinator
			c.TaskIdCounter++
			c.NumPendingMapTasks--

			var workerDetails = new (WorkerDetails)
			workerDetails.workerId = args.CurrentWorkerId
			workerDetails.workerNum = c.TaskIdCounter
			c.AssignedMapTasks[filename] = workerDetails
			
			c.MapTaskList[filename] = InProgress

			// Build the reponse
			reply.TaskId = c.TaskIdCounter
			reply.TaskType = Map
			reply.MapFileName = filename
			reply.ReplyStatus = TaskAssigned

			// need to figure out a better logic for worker number.
			// (maybe keep another global variable?)
			// currently just assigning it to taskid
			// will be important during failures
			reply.MapWorkerNum = c.TaskIdCounter

			// need to schedule a goroutine that will sleep
			// for 10 seconds to wait for this worker to
			// complete.
			// go WaitForWorker(workerDetails)

		} else {
			
			// no map task is unassigned, but all map tasks have not
			// yet completed, so worker must wait until they do
			reply.ReplyStatus = WaitForMoreTasks
		}
		
	} else if c.NumPendingReduceTasks > 0 {

		// all map tasks have completed.
		// begin reduce functions
		
	} else {
		
		// all tasks have completed. you can ask this worker to exit
		reply.ReplyStatus = -1
	}

	c.MuLock.Unlock()

	
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


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	
	trace()
	// Initialise the coordinator struct
	c := Coordinator{
		MapTaskList: make(map[string]int),
		ReduceTaskList: make(map[int]int),
		AssignedMapTasks: make(map[string]*WorkerDetails),
		AssignedReduceTasks: make(map[int]WorkerId),
		TaskIdCounter: 0,
	}

	c.MuLock.Lock()

	
	fmt.Println("Initializing Coordinator Maps...")
	for _, fileName := range files {

		c.MapTaskList[fileName] = Unassigned
	}

	for i := 0; i < nReduce; i++ {

		c.ReduceTaskList[i] = Unassigned
	}

	c.NumPendingMapTasks = len(files)
	c.NumPendingReduceTasks = nReduce

	showPendingTasks(c.NumPendingMapTasks, c.NumPendingReduceTasks)
	
	c.MuLock.Unlock()

	c.server()
	return &c
}
