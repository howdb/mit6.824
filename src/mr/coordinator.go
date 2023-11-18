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
)

type JobState int
const (
	Mapping JobState = iota
	Reducing
	Done
)
type Coordinator struct {
	// Your definitions here.
	State JobState
	NReduce int
	MapTasks []*MapTask
	ReduceTasks []*ReduceTask

	MappedTaskId map[int]struct{}
	AssignId int
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret = c.State == Done

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		AssignId: 0,
		NReduce: nReduce,
		MappedTaskId: make(map[int]struct{}),
	}

	// Your code here.
	for _, filename := range files {
		c.MapTasks = append(c.MapTasks, &MapTask{TaskMeta: TaskMeta{State: Pending}, Filename: filename})
	}
	// fmt.Printf("len of c.mapTasks %d\n", len(c.MapTasks))

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &ReduceTask{TaskMeta: TaskMeta{State: Pending, Id: i}})
	}
	c.State = Mapping

	c.server()
	return &c
}

const TIMEOUT = 10 * time.Second
func (c *Coordinator) AssignTask(_ *PlaceHolder, reply *Task) error {
	reply.Operation = ToWait
	if c.State == Mapping {
		for _, task := range c.MapTasks {
			c.mu.Lock()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(time.Now()) {
				task.State = Pending
			}
			if task.State == Pending {
				task.State = Executing
				task.StartTime = time.Now()
				c.AssignId++
				task.Id = c.AssignId
				c.mu.Unlock()
				fmt.Printf("assign map task %d\n", task.Id)

				reply.Operation = ToRun
				reply.IsMap = true
				reply.Map = *task
				reply.NReduce = c.NReduce
				return nil
			}
			c.mu.Unlock()
		}
	}
	if c.State == Reducing {
		for _, task := range c.ReduceTasks {
			c.mu.Lock()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(time.Now()) {
				task.State = Pending
			}
			if task.State == Pending {
				task.State = Executing
				task.StartTime = time.Now()
				task.IntermediateFilenames = nil
				for id := range c.MappedTaskId {
					task.IntermediateFilenames = append(task.IntermediateFilenames, fmt.Sprintf("mr-%d-%d", id, task.Id))
				}
				c.mu.Unlock()
				fmt.Printf("assign reduce task %d\n", task.Id)

				reply.Operation = ToRun
				reply.IsMap = false
				reply.NReduce = c.NReduce
				reply.Reduce = *task

				return nil
			}
			c.mu.Unlock()
		}
	}
	return nil
}

func (c *Coordinator) Finish(args *FinishArgs, _ *PlaceHolder) error {
	if args.IsMap {
		for _, task := range c.MapTasks {
			// c.mu.Lock()
			if task.Id == args.Id {
				task.State = Finished
				c.MappedTaskId[task.Id] = struct{}{}
				break
			}
		}
		fmt.Printf("map task finished %d\n", args.Id)
		for _, task := range c.MapTasks {
			if task.State != Finished {
				return nil
			}
		}
		c.State = Reducing
	} else {
		fmt.Printf("call reduce finish\n")
		for _, task := range c.ReduceTasks {
			// c.mu.Lock()
			if task.Id == args.Id {
				task.State = Finished
				break
			}
		}
		fmt.Printf("reduce task finished %d\n", args.Id)
		for _, task := range c.ReduceTasks {
			if task.State != Finished {
				return nil
			}
		}
		c.State = Done
	}
	return nil
}