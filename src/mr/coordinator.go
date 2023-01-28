package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"runtime"
	"sync"
	"time"
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
	MapTaskList map[string]*TaskDetails

	//
	// ReduceTaskList: Full list of all the Reduce Tasks and
	// their status maintained by the Coordinator
	//
	ReduceTaskList map[int]*TaskDetails

	//
	// PendingMapTasks: number of map tasks pending completion
	//
	PendingMapTasks int

	//
	// PendingReduceTasks: number of reduce tasks pending completion
	//
	PendingReduceTasks int

	//
	// TaskIdCounter: current value gives us the Task Id
	//
	TaskIdCounter int

	//
	// CompletedWorkerList
	//
	CompletedWorkerList []int

	//
	// ReduceBuckets: total number of reduce workers available
	//
	ReduceBuckets int
	
	//
	// MuLock: synchronise access to Coordinator struct
	//
	MuLock sync.Mutex
}

type TaskDetails struct {

	id int
	status int
	workerId WorkerId
	workerNum int
}
	
	

func trace() (string, string, int) {

	pc, _ , line, ok := runtime.Caller(1)
	
	if !ok {
		return "%s:%d\n", "?", 0
	}

	fn := runtime.FuncForPC(pc)
	
	if fn == nil {
		return "%s:%d\n", "?", 0
	}

	return "%s():%d\n", fn.Name(), line
}



func showPendingTasks(numMap int, numReduce int) {
	
	fmt.Printf("Number of map tasks pending: %d\n", numMap)

	fmt.Printf("Number of reduce tasks pending: %d\n", numReduce)
}



func (c *Coordinator) getUnassignedMapTask () (bool, string) {

	// log.Printf(trace())
	
	var unassigned = false
	var filename = ""
	var task *TaskDetails
	
	for filename, task = range c.MapTaskList {

		// we'll pick the first unassigned task we find
		if task.status == Unassigned {
			unassigned = true
			break
		}
	}

	return unassigned, filename
}



func (c *Coordinator) getUnassignedReduceTask () (bool, int) {

	// log.Printf(trace())

	var unassigned = false
	var reduceTaskNum = -1

	var task *TaskDetails

	for reduceTaskNum, task = range c.ReduceTaskList {

		if task.status == Unassigned {
			unassigned = true
			break
		}
	}
		
	return unassigned, reduceTaskNum
}



func (c *Coordinator) WaitForMapWorker(fileName string) {

	// log.Printf(trace())
	
	// The map task has just been scheduled.
	// Wait for 10 seconds 
	time.Sleep(time.Second * 10)

	c.MuLock.Lock()

	defer c.MuLock.Unlock()

	task := c.MapTaskList[fileName]

	if task.status == InProgress {
		// log.Printf("Worker %d timed out. Rescheduling task.\n", task.workerNum)
		// something went wrong. worker hasn't
		// yet completed. Mark task as unassigned
		// so the coorindator can assign it to
		// the next worker that requests a task
		task.status = Unassigned
	} 

	if task.status == Complete {
		// log.Printf("Worker %d successfully completed task\n", task.workerNum)
	}
}



func (c *Coordinator) WaitForReduceWorker(reduceNum int) {

	// log.Printf(trace())
	
	// The reduce task has just been scheduled.
	// Wait for 10 seconds 
	time.Sleep(time.Second * 10)

	c.MuLock.Lock()

	defer c.MuLock.Unlock()

	task := c.ReduceTaskList[reduceNum]

	if task.status == InProgress {

		// log.Printf("Worker %d timed out. Rescheduling task.\n", task.workerNum)
		
		// something went wrong. worker hasn't
		// yet completed. Mark task as unassigned
		// so the coorindator can assign it to
		// the next worker that requests a task
		task.status = Unassigned
	}

	if task.status == Complete {
		// log.Printf("Worker %d successfully completed task\n", task.workerNum)
	}
	
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

	// log.Printf(trace())
	// log.Printf("Requesting worker id: %d\n", args.CurrentWorkerId)
	
	c.MuLock.Lock()

	defer c.MuLock.Unlock()

	if c.PendingMapTasks > 0 {

		// map tasks have not yet completed. assign map tasks

		unassignedTaskPresent, filename :=
			c.getUnassignedMapTask()
		
		// Ask if there are unassigned map tasks
		if (unassignedTaskPresent) {

			// log.Printf("Unassigned task present = %t, filename = %s\n", unassignedTaskPresent, filename)

			// Update the coordinator
			c.TaskIdCounter++

			task := c.MapTaskList[filename]

			task.id = c.TaskIdCounter
			task.status = InProgress
			task.workerId = args.CurrentWorkerId
			// currently we're keep taskid the same as worker id.
			// might change it to separate later. at the moment, they
			// both move in lockstep
			task.workerNum = c.TaskIdCounter


			// log.Printf("Task Details: %v\n\n", task)

			// Build the reponse
			reply.TaskId = c.TaskIdCounter
			reply.TaskType = Map
			reply.MapFileName = filename
			reply.ReplyStatus = TaskAssigned
			reply.MapWorkerNum = c.TaskIdCounter
			reply.ReduceBuckets = c.ReduceBuckets
			
			// need to schedule a goroutine that will sleep
			// for 10 seconds to wait for this worker to
			// complete.
			go c.WaitForMapWorker(filename)

		} else {
			
			// no map task is unassigned, but all map tasks have not
			// yet completed, so worker must wait until they do
			reply.ReplyStatus = WaitForMoreTasks
		}
		
	} else if c.PendingReduceTasks > 0 {

		unassignedTaskPresent, reduceWorkerNumber :=
			c.getUnassignedReduceTask()
		
		// all map tasks have completed.
		// begin reduce functions
		if (unassignedTaskPresent) {

			// log.Printf("Unassigned task present = %t, reduce no = %d\n",	unassignedTaskPresent, reduceWorkerNumber)
			
			// Update the coordinator
			c.TaskIdCounter++

			task := c.ReduceTaskList[reduceWorkerNumber]

			task.id = c.TaskIdCounter
			task.status = InProgress
			task.workerId = args.CurrentWorkerId
			// currently we the taskid is the same as worker id.
			// might change it to separate later. at the moment, 
			// they both move in lockstep
			task.workerNum = c.TaskIdCounter


			// Build the response
			reply.TaskId = c.TaskIdCounter
			reply.TaskType = Reduce
			reply.ReduceWorkerNum = reduceWorkerNumber
			reply.ReplyStatus = TaskAssigned
			reply.ReduceBuckets = c.ReduceBuckets
			reply.CompletedMapWorkers = c.CompletedWorkerList

			// schedule a goroutine that will sleep for 10 seconds
			// to wait for this worker to complete
			go c.WaitForReduceWorker(reduceWorkerNumber)
		} else {
			
			// no map task is unassigned, but all map tasks have not
			// yet completed, so worker must wait until they do
			reply.ReplyStatus = WaitForMoreTasks
		}
		
		
	} else {
		
		// all tasks have completed. you can ask this worker to exit
		reply.ReplyStatus = SafelyExit
	}

	return nil
}


func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {

	// log.Printf(trace())
	// first check task type

	// then check appropriate data structure for whether
	// task has been reassigned ie current completion is
	// a stale completion because the coordinator already
	// timed out. ReplyStatus = 3

	// if no stale completion, mark task as complete.
	// decrease pending counter. Based on pending counter,
	// ReplyStatus = -1 or 1

	var task *TaskDetails

	c.MuLock.Lock()

	defer c.MuLock.Unlock()

	switch args.TaskType {

	case Map:
		task = c.MapTaskList[args.MapFilename]
	case Reduce:
		task = c.ReduceTaskList[args.ReduceWorkerNum]
	}

	if (task.id == args.TaskId) && (task.status == InProgress) {


		if !args.TaskSucceeded {

			// worker faced an error while servicing the task
			// mark it as unassigned and move on.
			task.status = Unassigned
			reply.ReplyStatus = MoreTasks

		} else {
		
			// task completed successfully
			task.status = Complete


			switch args.TaskType {
			case Map: 
				c.PendingMapTasks--
				// add map worker to completed map worker list
				c.CompletedWorkerList =
					append(c.CompletedWorkerList,
						task.workerNum)
				
			case Reduce:
				c.PendingReduceTasks--
			}


			if (c.PendingMapTasks == 0) &&
				(c.PendingReduceTasks == 0) {
			
				reply.ReplyStatus = SafelyExit
			} else {
				
				reply.ReplyStatus = MoreTasks 
			}
		}

	} else {
		 
		if task.id != args.TaskId {
			
			// If the two task Ids don't match, the coordinator
			// has already assigned this to another worker
			// ie stale completion. discard worker data
			reply.ReplyStatus = StaleTaskCompletion

		} else if task.status == Unassigned {
			// ASSERT (task.id > args.TaskId)
			// the coordinator timed out and has marked this
			// task to be reassigned to another worker.
			// ie stale completion. discard worker data
			reply.ReplyStatus = StaleTaskCompletion

		} else {
			// ASSERT (task.status == Complete)
			// ignore if task.status == Complete. it just
			// means the worker didn't get the TaskCompleteReply
			reply.ReplyStatus = MoreTasks
		}
	}
	
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {

	// log.Printf(trace())
	
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

	c.MuLock.Lock()

	defer c.MuLock.Unlock()

	mapComplete := c.PendingMapTasks == 0
	reduceComplete := c.PendingReduceTasks == 0

	return mapComplete && reduceComplete
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	
	// log.Printf(trace())

	// Initialise the coordinator struct
	c := Coordinator{
		MapTaskList: make(map[string]*TaskDetails),
		ReduceTaskList: make(map[int]*TaskDetails),
		TaskIdCounter: -1,
		CompletedWorkerList: make([]int, 0, len(files)), // should be an extendable array
	}


	c.MuLock.Lock()

	
	// log.Println("Initializing Coordinator Maps...")
	for _, fileName := range files {

		// new() zeroes out the allocated data which works for us
		c.MapTaskList[fileName] = new (TaskDetails)
		
	}

	for reduceNum := 0; reduceNum < nReduce; reduceNum++ {

		// new() zeroes out the allocated data which works for us
		c.ReduceTaskList[reduceNum] = new (TaskDetails)
	}

	c.ReduceBuckets = nReduce
	
	c.PendingMapTasks = len(files)
	c.PendingReduceTasks = nReduce
	
	c.MuLock.Unlock()

	c.server()
	return &c
}
