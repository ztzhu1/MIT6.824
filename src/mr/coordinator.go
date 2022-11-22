package mr

import (
	// "fmt"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"6.824/assert"
)

// import "sync"


type Coordinator struct {
	// Your definitions here.
	nReduce          int
	files            []string
	tasks_unassigned chan *Task
	tasks_assigned   chan *Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) generateMapTasks() {
	for i, file := range c.files {
		task := new(Task)
		task.Type = MAP
		task.Id = c.getTaskId(file)

		task.InputName = file
		task.OutputName = "mr-%v-%v"
		task.OutputName = fmt.Sprintf(task.OutputName, task.Id, i)

		c.tasks_unassigned <- task
	}
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	var ok bool
	reply.Task_, ok = <- c.tasks_unassigned
	c.tasks_assigned <- reply.Task_
	assert.Assert(ok)
	return nil
}

func (c *Coordinator) getTaskId(key string) int {
	return ihash(key) % c.nReduce
}

func (c *Coordinator) getTaskNum() int {
	return len(c.files)
}
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


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	/**
	 * Pass parameters
	 */
	c.nReduce = nReduce

	fileNum := len(files)
	c.files = make([]string, fileNum)
	assert.Assert(copy(c.files, files) == fileNum)

	c.tasks_unassigned = make(chan *Task, c.getTaskNum())
	c.tasks_assigned   = make(chan *Task, c.getTaskNum())

	c.generateMapTasks()
	/**
	 * Start listening
	 */
	c.server()

	return &c
}
