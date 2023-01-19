package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


/**********************************************/
/*                 Constants                  */
/**********************************************/

const (
	
	//
	// Task Type
	//
	Map = 0
	Reduce = 1

	//
	// Reply Status
	//
	SafelyExit = -1
	MoreTasks = 0
	WaitForMoreTasks = 1
	TaskAssigned = 2
)

type WorkerId int 

/**********************************************/
/*                 Datatypes                  */
/**********************************************/

//
// Request Arguments for AssignTask()
//
type AssignTaskArgs struct {

	//
	// WorkerId: Id used to identify the worker
	//
	CurrentWorkerId WorkerId
}


//
// Response Arguments for AssignTask()
//
type AssignTaskReply struct {

	//
	// ReplyStatus: tells us what the Coordinator wants us to do
	//
	// -1: no more tasks. safely exit
	//  0: tasks exist but can't be assigned yet. wait and ask again
	//  1: task is assigned. check other fields of struct for more info
	//
	ReplyStatus int

	//
	// TaskId: sent by Coordinator and is the Id to
	// send back to the Coordinator when a task is completed
	// Field must only be checked when ReplyStatus == 1
	//
	TaskId int

	//
	// TaskType: tells us the type of task that the Coordinator has assigned
	//
	//    Map / 0: map task
	// Reduce / 1: reduce task 
	//
	TaskType int

	//
	// MapWorkerNum: tells the worker what is the X in mr-X-Y
	//
	MapWorkerNum int

	//
	// MapFileName: input file for which we need to run map function. Only
	// valid when the TaskType == Map 
	//
	MapFileName string

	//
	// ReduceWorkerNum: the reduce worker number assigned to this worker. Only
	// valid when the TaskType == Reduce 
	//
	ReduceWorkerNum int
}


//
// Request Arguments for TaskComplete()
//
type TaskCompleteArgs struct {

	//
	// TaskType: tells the Coordinator what type of task has completed
	//
	//    Map / 0: map task
	// Reduce / 1: reduce task 
	//
	TaskType int
	
	//
	// MapWorkerNum: tells the Coordinator which Map worker has completed
	//
	MapWorkerNum int

	//
	// ReduceWorkerNum: tells the Coordinator which Reduce worker has completed
	//
	ReduceWorkerNum int

	//
	// TaskId: tells the Coordinator what is the Id of the 
	// task that is completed
	//
	TaskId int
}

type TaskCompleteReply struct {

	//
	// ReplyStatus: tells us what the Coordinator wants us to do
	//
	// -1: no more tasks. safely exit
	//  0: tasks exist but can't be assigned yet. wait and ask again
	//
	ReplyStatus int
}



/**********************************************/
/*                   Methods                  */
/**********************************************/



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
