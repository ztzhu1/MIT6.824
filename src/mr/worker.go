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

	for cond := true; cond; {
		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		assert.Assert(ok)

		task := reply.Task_

		fmt.Println(task.Type, task.ID, task.InputName, task.OutputName)
		if task.Type == MAP {
			contentPtr := read(task.InputName)
			ofileTempName := mapHelper(task.InputName,
									   task.OutputName,
									   contentPtr,
									   mapf)
			// notify the coordinator this worker has
			// completed its task
			notify(task, &ofileTempName, &args, &reply)
		} else if task.Type == REDUCE {
			ofileTempName := reduceHelper(task.InputName,
										  task.OutputName,	
										  reducef)
			// notify the coordinator this worker has
			// completed its task
			notify(task, &ofileTempName, &args, &reply)
		} else {
			fmt.Printf("\033[1;32m")
			fmt.Printf("QUIT")
			fmt.Printf("\033[0m")

			cond = false
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
		log.Fatal("dialing:", err)
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
			   mapf func(string, string) []KeyValue) string {
	kva := mapf(inputName, *contentPtr)
	sort.Sort(ByKey(kva))

	ofile, _ := os.CreateTemp("./", outputName + "-")
	enc := json.NewEncoder(ofile)

	for _, kv := range kva {
		enc.Encode(&kv)
	}
	ofile.Close()

	return ofile.Name()
}

func reduceHelper(inputPattern string,
				  outputName string,
				  reducef func(string, []string) string) string {
	mapFiles, _ := filepath.Glob(inputPattern)
	if mapFiles == nil {
		return ""
	}

	kva := []KeyValue{}
	for _, mapFile := range mapFiles {
		kva = append(kva, reduceOne(mapFile, reducef)...)
	}
	sort.Sort(ByKey(kva))

	ofile, _ := os.CreateTemp("./", outputName + "-")

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		sum := 0
		for k := i; k < j; k++ {
			v, err := strconv.Atoi(kva[k].Value)
			assert.Assert(err == nil)
			sum += v
		}
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, sum)

		i = j
	}

	ofile.Close()

	return ofile.Name()
}

func reduceOne(mapFile string, reducef func(string, []string) string) []KeyValue {
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

	result := []KeyValue{}
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

		// this is the correct format for each line of Reduce output.
		// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		result = append(result, KeyValue{kva[i].Key, output})

		i = j
	}
	return result
}

func notify(task *Task,
			ofileTempName *string,
			args *TaskArgs,
			reply *TaskReply) {
	args.TaskID = task.ID
	args.TempName = *ofileTempName
	args.Name = task.OutputName
	ok := call("Coordinator.CompleteTask", args, reply)
	assert.Assert(ok)
}