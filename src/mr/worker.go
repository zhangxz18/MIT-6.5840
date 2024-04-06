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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	finish_task_args := AskForTaskArgs{TaskNo, []int{}, []string{}}
	ask_for_task_reply := AskForTaskReply{}
	// Your worker implementation here.
	for {
		call_result := CallAskForTask(&finish_task_args, &ask_for_task_reply)
		if (!call_result) {
			break
		}
		finish_task_args.FinishedTaskType = TaskNo
		finish_task_args.FinishTaskList = []int{}
		finish_task_args.FinishedTaskFile = []string{}
		if ask_for_task_reply.DispatchedTaskType == TaskNo{
			time.Sleep(1000000) // sleep 1ms	
		} else if ask_for_task_reply.DispatchedTaskType == TaskMap{
			DoMapTask(mapf, &finish_task_args, &ask_for_task_reply)
		} else if ask_for_task_reply.DispatchedTaskType == TaskReduce{
			DoReduceTask(reducef, &finish_task_args, &ask_for_task_reply)
		} else if ask_for_task_reply.DispatchedTaskType == TaskFinish{
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func DoMapTask(mapf func(string, string) []KeyValue, finish_task *AskForTaskArgs,assigned_task *AskForTaskReply){
	for i, filename := range assigned_task.DispatchedTaskFile{
		intermediates := make([][]KeyValue, assigned_task.ReduceNum)
		for j := range intermediates{
			intermediates[j] = []KeyValue{}
		}
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva{
			intermediates[ihash(kv.Key)%assigned_task.ReduceNum] = append(intermediates[ihash(kv.Key)%assigned_task.ReduceNum], kv)
		}
		for j := range intermediates{
			// To avoid two workers write to the same file, map-reduce create a temp file and rename it after finishing work
			// filename = mr-<map_task_no>-<reduce_task_no>
			filename = "mr-" + strconv.Itoa(assigned_task.DispatchedTaskList[i]) + "-" + strconv.Itoa(j)
			file, err := ioutil.TempFile("", filename)
			if err != nil {
				log.Fatalf("cannot create %v", filename)
			}
			sort.Sort(ByKey(intermediates[j]))
			enc := json.NewEncoder(file)
			for _, kv := range intermediates[j] {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode %v", &kv)
				}
			}
			os.Rename(file.Name(), filename)
			file.Close()
		}
		finish_task.FinishedTaskFile = append(finish_task.FinishedTaskFile, filename)
		finish_task.FinishTaskList = append(finish_task.FinishTaskList, assigned_task.DispatchedTaskList[i])
	}

	finish_task.FinishedTaskType = TaskMap
}

func DoReduceTask(reducef func(string, []string) string, finish_task *AskForTaskArgs,assigned_task *AskForTaskReply){
	for _, reduce_task_id := range assigned_task.DispatchedTaskList{
		intermediate := []KeyValue{}
		filenames, _ := filepath.Glob("mr-*-" + strconv.Itoa(reduce_task_id))
		for _, filename := range filenames {
			// fmt.Println(filename)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for{
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			file.Close()
		}
		sort.Sort(ByKey(intermediate))

		oname := "mr-out-" + strconv.Itoa(reduce_task_id)
		// ofile, err = os.Create(oname)
		ofile, err := ioutil.TempFile("", oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}

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
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
	
			i = j
		}
		os.Rename(ofile.Name(), oname)
		ofile.Close()
		finish_task.FinishedTaskFile = append(finish_task.FinishedTaskFile, oname)
		finish_task.FinishTaskList = append(finish_task.FinishTaskList, reduce_task_id)
	}
	finish_task.FinishedTaskType = TaskReduce
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

func CallAskForTask(task_args *AskForTaskArgs, task_reply *AskForTaskReply) bool{
	ok := call("Coordinator.AskForTask", task_args, task_reply)
	if ok {
		// fmt.Printf("reply.tasktype %v\n", task_reply.DispatchedTaskType)
		return true
	} else {
		fmt.Printf("call failed!\n")
		return false
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

	fmt.Println(err)
	return false
}
