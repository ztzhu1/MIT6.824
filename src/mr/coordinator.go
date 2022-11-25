package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"6.824/assert"
)

const taskTimeOut    = 10 * 1000 // 10s
const channelTimeOut =  5 * 1000 //  5s

type Coordinator struct {
	// Your definitions here.
	nReduce          int
	tasks_unassigned chan *Task
	map_tasks        [] *Task
	reduce_tasks     [] *Task
	mu               sync.Mutex
	mapDone          bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Can't use c.Done(), or dead lock occurs.
	done := len(c.reduce_tasks) == 0
	if !done {
		var ok bool
		select {
		case reply.Task_, ok = <- c.tasks_unassigned:
			reply.Task_.processing = true
		case <-time.After(channelTimeOut * time.Millisecond):
			task := new(Task)
			task.Type = FAKE
			reply.Task_ = task
			return nil
		}
		assert.Assert(ok)
	} else {
		task := new(Task)
		task.Type = FAKE
		reply.Task_ = task
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.mapDone {
	// check map task
		for i := 0; i < len(c.map_tasks); i++ {
			if c.map_tasks[i].ID == args.TaskID {
				// rename temp file
				err := os.Rename(args.TempName, args.Name)
				assert.Assert(err == nil)
				// pop the task which was just completed
				c.map_tasks = append(c.map_tasks[:i], c.map_tasks[i+1:]...)
				break
			}
		}

		if len(c.map_tasks) == 0 {
		// all map tasks are done
			c.mapDone = true
			c.pushTask(c.reduce_tasks)
		}

		// remove temp file anyway
		os.Remove(args.TempName)
	} else {
	// check reduce task
		for i := 0; i < len(c.reduce_tasks); i++ {
			if c.reduce_tasks[i].ID == args.TaskID {
				// rename temp file
				if args.TempName != "" {
					err := os.Rename(args.TempName, args.Name)
					assert.Assert(err == nil)
				}
				// pop the task which was just completed
				c.reduce_tasks = append(c.reduce_tasks[:i], c.reduce_tasks[i+1:]...)
				break
			}
		}

		// remove temp files anyway
		fileNameSlice := strings.Split(args.Name, "-")
		pattern := "mr-*-" + fileNameSlice[len(fileNameSlice) - 1]
		tempFiles, _ := filepath.Glob(pattern)
		for _, tempFile := range tempFiles {
			if strings.Split(tempFile, "-")[1] != "out" {
				os.Remove(tempFile)
			}
		}
	}
	return nil
}

// private method
func (c *Coordinator) generateMapTasks(files []string) {
	for _, file := range files {
		task := new(Task)
		task.Type = MAP
		task.ID = ihash(file)
		task.processing = false
		task.procTime = 0

		task.InputName  = file
		task.OutputName = "mr-%v-%v"
		task.OutputName = fmt.Sprintf(task.OutputName, task.ID, task.ID % c.nReduce)

		c.map_tasks = append(c.map_tasks, task)
	}
}

func (c *Coordinator) generateReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		task := new(Task)
		task.Type = REDUCE
		task.ID = i 
		task.processing = false
		task.procTime = 0

		task.InputName  = "mr-*-" + strconv.Itoa(i)
		task.OutputName = "mr-out-%v"
		task.OutputName = fmt.Sprintf(task.OutputName, i)

		c.reduce_tasks = append(c.reduce_tasks, task)
	}
}

func (c *Coordinator) pushTask(tasks [] *Task) {
	for _, task := range tasks {
		c.tasks_unassigned <- task
	}
}

func max(a, b int) int {
	if (a > b) {
		return a
	}
	return b
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
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	done := len(c.reduce_tasks) == 0
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(patterns []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	/**
	 * Pass parameters
	 */
	c.nReduce = nReduce
	c.mapDone = false

	/**
	 * Generate tasks
	 */
	var files []string
	for _, pattern := range patterns {
		matches, _ := filepath.Glob(pattern)
		files = append(files, matches...)
	}
	c.generateMapTasks(files)
	c.generateReduceTasks()

	/**
	 * Make channel
	 */
	c.tasks_unassigned = make(
		chan *Task,
		max(len(c.map_tasks), len(c.reduce_tasks)))
	/**
	 * Push map tasks into channel.
	 * Reduce tasks will be pushed after map tasks are done
	 */
	c.pushTask(c.map_tasks)

	/**
	 * Start listening
	 */
	c.server()

	return &c
}

func (c *Coordinator) Tick(elapsed_ms int64) {
	var tasks [] *Task

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.mapDone {
		tasks = c.map_tasks
	} else {
		tasks = c.reduce_tasks
	}

	for _, task := range tasks {
		if (task.processing) {
			task.procTime += elapsed_ms
			// if timeout, re-assign the task
			if task.procTime > taskTimeOut {
				task.procTime = 0
				task.processing = false
				c.tasks_unassigned <- task
			}
		}
	}
	
}