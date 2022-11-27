package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"io/ioutil"
	"strconv"
	"sync"
	"time"
	"regexp"

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
	cleanDone        bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.NReduce = c.nReduce
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
				for j, tempName := range args.TempNames {
					err := os.Rename(tempName, args.Name + strconv.Itoa(j))
					assert.Assert(err == nil)
				}
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
	} else {
	// check reduce task
		for i := 0; i < len(c.reduce_tasks); i++ {
			if c.reduce_tasks[i].ID == args.TaskID {
				// rename temp file
				if args.TempNames[0] != "" {
					err := os.Rename(args.TempNames[0], args.Name)
					assert.Assert(err == nil)
				}
				// pop the task which was just completed
				c.reduce_tasks = append(c.reduce_tasks[:i], c.reduce_tasks[i+1:]...)
				break
			}
		}
	}
	// clean temp files after all tasks are done
	done := len(c.reduce_tasks) == 0
	if done && !c.cleanDone {
		c.removeAllTempFiles()
		c.cleanDone = true
	}
	return nil
}

// private method
func (c *Coordinator) generateMapTasks(files []string) {
	for i, file := range files {
		task := new(Task)
		task.Type = MAP
		task.ID = i
		task.processing = false
		task.procTime = 0

		task.InputName  = file
		task.OutputName = "mr-%v-"
		task.OutputName = fmt.Sprintf(task.OutputName, task.ID)

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

		task.InputName  = "mr-*-%v"
		task.InputName  = fmt.Sprintf(task.InputName, i)
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

func (c *Coordinator) removeAllTempFiles() {
	mapPattern, _    := regexp.Compile("mr-\\d+-\\d+")
	reducePattern, _ := regexp.Compile("mr-out-\\d+-\\d+")
	files, _ := ioutil.ReadDir("./")
	for _, tempFile := range files {
		if mapPattern.MatchString(tempFile.Name()) ||
           reducePattern.MatchString(tempFile.Name()) {
			os.Remove(tempFile.Name())
		}
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
	c.nReduce   = nReduce
	c.mapDone   = false
	c.cleanDone = false

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
				fmt.Println("\033[1;33m")
				fmt.Println("reassign:", task.Type, task.ID, task.InputName, task.OutputName)
				fmt.Println("\033[0m")
			}
		}
	}
	
}