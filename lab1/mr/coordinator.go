package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

// 2- phase
const (
	PhaseMAP = iota
	PhaseReduce
)

// Task Status
const (
	UNDO = iota
	WAITING
	DONE
)

const TimeLimit = 10

type TaskAll struct {
	TaskType int
	TaskID   int
	File     []string
	status   int
	start    int64
}

type Coordinator struct {
	// Your definitions here.
	tasks []TaskAll
	mu    sync.Mutex
	phase int
	done  int
	total []int
}

// Your code here -- RPC handlers for the worker to call.

//// Example
//// an example RPC handler.
//// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// Get Coordinator.Get
// worker talk to Coordinator to get a task
// if there exists none, hang up on the container
func (c *Coordinator) Get(arg Ignore, resp *Task) error {
	fmt.Println("COORDINATOR:--", "[Recv]: a worker asks for a task...")
	c.mu.Lock()
	defer c.mu.Unlock()
	total := c.total[c.phase]
	types := []string{"MAP", "REDUCE"}
	offset := 0
	if c.phase == PhaseReduce {
		offset = c.total[PhaseMAP]
	}
	for i := 0; i < total; i++ {
		i += offset
		if c.tasks[i].status == UNDO {
			resp.TaskID = c.tasks[i].TaskID
			resp.TaskType = c.tasks[i].TaskType
			resp.Reducer = c.total[PhaseReduce]
			resp.File = c.tasks[i].File
			// recorde info
			c.tasks[i].status = WAITING
			c.tasks[i].start = time.Now().Unix()
			fmt.Println("COORDINATOR:--", "[Assign]:", types[resp.TaskType], " ", i-offset, " task...")
			return nil
			// if there is a straggler
		} else if c.tasks[i].status == WAITING && time.Now().Unix()-c.tasks[i].start >= TimeLimit {
			resp.TaskID = c.tasks[i].TaskID
			resp.TaskType = c.tasks[i].TaskType
			resp.Reducer = c.total[PhaseReduce]
			resp.File = c.tasks[i].File
			// recorde info
			//c.tasks[i].status = WAITING
			c.tasks[i].start = time.Now().Unix()
			fmt.Println("COORDINATOR:--", "[Assign]: a straggler ", types[resp.TaskType], " ", i-offset, "task....")
			return nil
			// if all tasks are done
			// if the DS is worked as coordinator distributes tasks
			// the hanging up goroutine doesn't need to check whether done or not
		}
		i -= offset
	}
	resp.TaskID = -1
	resp.TaskType = NONE
	resp.Reducer = -1
	resp.File = []string{}
	return nil
	//if c.reduceStart {
	//	c.mu.Unlock()
	//	resp.TaskID = -1
	//	resp.TaskType = -1
	//	resp.Reducer = -1
	//	resp.File = []string{}
	//} else {
	//	c.mu.Unlock()
	//	return c.Get(arg, resp)
	//}
	//return nil
}

// Finish
// if a map function done, call finish RPC to remainder coordinator
func (c *Coordinator) Finish(arg Finish, resp *Ignore) error {
	fmt.Println("COORDINATOR:--", "[Recv]: a msg of ", arg.TaskID, "-task finished")
	c.mu.Lock()
	defer c.mu.Unlock()
	c.done += 1
	if c.phase == PhaseMAP {
		c.tasks[arg.TaskID].status = DONE
		for i := 0; i < c.total[PhaseReduce]; i++ {
			c.tasks[c.total[PhaseMAP]+i].File = append(c.tasks[c.total[PhaseMAP]+i].File, arg.File[i])
		}
		if c.done == c.total[PhaseMAP] {
			fmt.Println("COORDINATOR:--", "Converting to a Reduce Phase")
			c.done = 0
			c.phase = PhaseReduce
		}
	} else if c.phase == PhaseReduce {
		fmt.Println("COORDINATOR:--", "coordinator set task ", arg.TaskID+c.total[PhaseMAP], " done...")
		c.tasks[arg.TaskID+c.total[PhaseMAP]].status = DONE
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].status != DONE {
			return false
		}
	}
	return true
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mu = sync.Mutex{}
	for i := 0; i < len(files); i++ {
		//fmt.Println("COORDINATOR:--", "make coordinator: append MAP task-", i, " in position ", i)
		c.tasks = append(c.tasks, TaskAll{MAP, i, []string{files[i]}, UNDO, 0})
	}
	for i := 0; i < nReduce; i++ {
		//fmt.Println("COORDINATOR:--", "make coordinator: append MAP task-", i, " in position ", len(files)+i)
		c.tasks = append(c.tasks, TaskAll{REDUCE, i, []string{}, UNDO, 0})
	}
	c.phase = PhaseMAP
	c.done = 0
	c.total = []int{len(files), nReduce}

	//fmt.Println("COORDINATOR:--", "coordinator made done...")
	//fmt.Println("COORDINATOR:--", "total: ", c.total[0], "-", c.total[1])
	//fmt.Println("COORDINATOR:--", "tasks len: ", len(c.tasks))

	c.server()
	return &c
}
