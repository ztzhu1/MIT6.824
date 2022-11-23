package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"io/ioutil"
	"sort"
	"errors"
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
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	assert.Assert(ok)

	task := reply.Task_

	if task.Type == FAKE {
		fmt.Printf("\033[1;34m")
		fmt.Printf("Received fake task. Worker quited.\n")
		fmt.Printf("\033[0m")
	} else {
		fmt.Printf("\033[1;32m")
		fmt.Printf("id: %v, in: %v, out: %v\n", task.Id, task.InputName, task.OutputName)
		fmt.Printf("\033[0m")

		contentPtr := read(task.InputName)
		mapHelper(task.InputName,
				  task.OutputName + "-",
				  contentPtr,
				  mapf)
		// result := reduceHelper(kva, reducef)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
			   mapf func(string, string) []KeyValue) {
	kva := mapf(inputName, *contentPtr)
	sort.Sort(ByKey(kva))

	ofile, _ := os.CreateTemp("./", outputName)
	for _, kv := range kva {
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
	}
	ofile.Close()
	renameOrDrop(ofile.Name())
}

func reduceHelper(mapFile string, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	result := []KeyValue{}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		result = append(result, KeyValue{intermediate[i].Key, output})

		i = j
	}
}

func renameOrDrop(filename string) bool {
	var renamed bool
	file := lockfile.Lockfile(filename)
	err := file.TryLock()
	if err == nil {
		exist = true
	} else if errors.Is(err, os.ErrNotExist) {
		exist = false
	} else {
		assert.Assert(false)
	}
	return renamed
}