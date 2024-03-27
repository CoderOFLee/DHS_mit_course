package mr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		code := worker(mapf, reducef)
		if code == -1 {
			fmt.Println("WORKER:--", "error exit")
			os.Exit(-1)
		} else if code == 0 {
			fmt.Println("WORKER:--", "all work done")
			break
		} else if code == 2 {
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

// Worker
// main/mrworker.go calls this function.
func worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) int {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	task := Task{}
	ignore := Ignore{A: 0}
	success := call("Coordinator.Get", ignore, &task)
	if !success {
		return 0
	}

	if task.TaskType == MAP {
		fmt.Println("WORKER:--", "Success get a MAP task", task.TaskID)
		// map function only got one file
		// but for structure simplification, we set it to a slice
		bcontents, err := os.ReadFile(task.File[0])
		if err != nil {
			fmt.Println("WORKER:--", "reading file wrong because ", err)
			return -1
		}
		contents := string(bcontents)
		intermediate := mapf(task.File[0], contents)
		var files []*os.File
		var intermidateFile []string
		for i := 0; i < task.Reducer; i++ {
			partition := "Task" + strconv.Itoa(task.TaskID) + "-Partition" + strconv.Itoa(i)
			intermidateFile = append(intermidateFile, partition)
			newFile, err := os.Create(partition)
			if err != nil {
				fmt.Println("WORKER:--", "creating file wrong because ", err.Error())
				return -1
			}
			files = append(files, newFile)
		}
		// if for{} is in this function, manually use close() instead of defer
		defer func() {
			for i := 0; i < len(files); i++ {
				files[i].Close()
			}
		}()
		// sort intermediate KV
		sort.Sort(ByKey(intermediate))
		for _, kv := range intermediate {
			partition := ihash(kv.Key) % task.Reducer
			_, err = files[partition].WriteString(kv.Key + " " + kv.Value + "\n")
			if err != nil {
				fmt.Println("WORKER:--", "writing in file wrong because ", err.Error())
			}
		}

		//finished
		finished := Finish{
			TaskID: task.TaskID,
			File:   intermidateFile,
		}
		call("Coordinator.Finish", finished, &ignore)
		fmt.Println("WORKER:--", "Call Finish of MAP task", task.TaskID)
	} else if task.TaskType == REDUCE {
		fmt.Println("WORKER:--", "Success get a REDUCE task", task.TaskID)
		// reduce is to collect nReduce Partition file and output one outfile
		ret := map[string][]string{}
		keys := []string{}
		// nReduce corresponding to partition
		for i := 0; i < len(task.File); i++ {
			fd, _ := os.Open(task.File[i])
			reader := bufio.NewReader(fd)
			line, _, err := reader.ReadLine()
			for ; err != io.EOF; line, _, err = reader.ReadLine() {
				splits := strings.Split(strings.TrimRight(string(line), "\n"), " ")
				key := splits[0]
				value := splits[1]
				if _, ok := ret[key]; ok {
					ret[key] = append(ret[key], value)
				} else {
					ret[key] = []string{value}
					keys = append(keys, key)
				}
			}
			fd.Close()
		}

		sort.Strings(keys)
		fd, err := os.Create("mr-out-" + strconv.Itoa(task.TaskID))
		defer fd.Close()
		if err != nil {
			fmt.Println("WORKER:--", "create file wrong because ", err.Error())
			return -1
		}
		for _, k := range keys {
			_, err := fd.WriteString(k + " " + reducef(k, ret[k]) + "\n")
			if err != nil {
				fmt.Println("WORKER:--", "writing in file wrong because ", err.Error())
			}
		}

		//finish
		finished := Finish{
			TaskID: task.TaskID,
			File:   []string{"mr-out-" + strconv.Itoa(task.TaskID)},
		}
		call("Coordinator.Finish", finished, &ignore)

		fmt.Println("WORKER:--", "Call Finish of REDUCE task", task.TaskID)
	} else if task.TaskID == NONE {
		fmt.Println("WORKER:--", "NONE to hang on...")
		return 2
	}
	return 1
}

//// CallExample example function to show how to make an RPC call to the coordinator.
////
//// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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

	fmt.Println("WORKER:--", err)
	return false
}
