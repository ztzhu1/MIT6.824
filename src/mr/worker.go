package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"6.824/assert"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const waitTime       =  600 // 600 ms


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
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			return
		}
		// assert.Assert(ok)

		task := reply.Task_

		// fmt.Println(task.Type, task.ID, task.InputName, task.OutputName)
		if task.Type == MAP {
			contentPtr := read(task.InputName)
			ofileTempNames := mapHelper(task.InputName,
									    task.OutputName,
									    contentPtr,
									    reply.NReduce,
									    mapf)
			// notify the coordinator this worker has
			// completed its task
			notify(task, ofileTempNames, &args, &reply)
		} else if task.Type == REDUCE {
			ofileTempNames := reduceHelper(task.InputName,
										   task.OutputName,	
										   reducef)
			// notify the coordinator this worker has
			// completed its task
			notify(task, ofileTempNames, &args, &reply)
		} else if task.Type == REREQ {
			time.Sleep(waitTime * time.Millisecond)
		} else if task.Type == QUIT {
			return
		} else {
			assert.Assert(false)
		}
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
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("\033[1;31m")
	fmt.Println(err)
	fmt.Printf("\033[0m")
	return false
}

// private methods
func read(filename string) *string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	ret := string(content)
	return &ret
}

func mapHelper(inputName  string,
			   outputName string,
			   contentPtr *string,
			   nReduce    int,
			   mapf func(string, string) []KeyValue) *[]string {
	kva := mapf(inputName, *contentPtr)
	sort.Sort(ByKey(kva))

	var ofiles     []*os.File
	var encs       []*json.Encoder
	var tempNames  []string
	// create temp files
	for i := 0; i < nReduce; i++ {
		ofile, _ := os.CreateTemp("./", outputName + strconv.Itoa(i) + "-")
		enc := json.NewEncoder(ofile)
		ofiles = append(ofiles, ofile)
		encs = append(encs, enc)
		tempNames = append(tempNames, ofile.Name())
	}

	// write
	for _, kv := range kva {
		i := reduceIdx(kv.Key, nReduce)
		encs[i].Encode(&kv)
	}

	// close files
	for _, ofile := range ofiles {
		ofile.Close()
	}

	return &tempNames
}

func reduceHelper(inputPattern string,
				  outputName string,
				  reducef func(string, []string) string) *[]string {
	var tempNames []string

	mapFiles, _ := filepath.Glob(inputPattern)
	// filtering unwanted temp files
	mapFilesFiltered := []string{}
	for _, file := range mapFiles {
		if strings.Count(file, "-") == 2 {
			mapFilesFiltered = append(mapFilesFiltered, file)
		}
	}
	if mapFilesFiltered == nil {
		return &tempNames
	}

	kva := []KeyValue{}
	for _, mapFile := range mapFilesFiltered {
		kva = append(kva, loadOne(mapFile)...)
	}
	sort.Sort(ByKey(kva))

	ofile, _ := os.CreateTemp("./", outputName + "-")

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	tempNames = append(tempNames, ofile.Name())
	return &tempNames
}

func loadOne(mapFile string) []KeyValue {
	ifile, err := os.Open(mapFile)

	assert.Assert(err == nil)

	kva := []KeyValue{}
	dec := json.NewDecoder(ifile)
	for {
		var kv KeyValue
		if err = dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	ifile.Close()

	return kva
}

func reduceIdx(key string, nReduce int) int {
	return ihash(key) % nReduce
}

func notify(task *Task,
			ofileTempNames *[]string,
			args *TaskArgs,
			reply *TaskReply) {
	args.TaskID = task.ID
	args.TempNames = *ofileTempNames
	args.Name = task.OutputName
	call("Coordinator.CompleteTask", args, reply)
}