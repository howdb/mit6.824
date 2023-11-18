package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// switch JobType {
	// case MapJob:
	// 	mapf()
	// case ReduceJob:
	// 	reducef()
	// case Wait:
	// 	time.Sleep(1 * time.Millisecond)
	// case Finish:
	// }
	for {
		time.Sleep(1 * time.Second)
		task := Task{}
		call("Coordinator.AssignTask", &PlaceHolder{}, &task)
		if task.Operation == ToWait {
			continue
		}
		if task.IsMap {
			log.Printf("receive map job %v", task.Map.Filename)
			err := doMap(task, mapf)
			if err != nil {
				log.Fatalf("domap fail")
			}
		} else {
			log.Printf("receive reduce job %v %v", task.Reduce.Id, task.Reduce.IntermediateFilenames)
			err := doReduce(task, reducef)
			if err != nil {
				log.Fatalf("doreduce fail")
			}
		}
	}

}

type ByKey []KeyValue
func (a ByKey) Len() int { return len(a) }
func (a ByKey) Swap(x, y int) { a[x], a[y] = a[y], a[x] }
func (a ByKey) Less(x, y int) bool { return a[x].Key < a[y].Key }

type PlaceHolder struct{}
type FinishArgs struct {
	IsMap bool
	Id int
}

func doMap(task Task, mapf func(string, string) []KeyValue) error {
	filename := task.Map.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("file open fail %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	} 
	defer file.Close()
	kva := mapf(filename, string(content))

	var encoders []*json.Encoder
	for i := 0; i < task.NReduce; i++ {
		file, err := os.Create(fmt.Sprintf("mr-%d-%d", task.Map.Id, i))
		if err != nil {
			log.Fatalf("create file fail")
		}
		encoders = append(encoders, json.NewEncoder(file))
	}
	for _, kv := range kva {
		_ = encoders[ihash(kv.Key) % task.NReduce].Encode(&kv)
	}
	call("Coordinator.Finish", &FinishArgs{IsMap: true, Id: task.Map.Id}, &PlaceHolder{})
	return nil
}

func doReduce(task Task, reducef func(string, []string) string) error {
	kva := []KeyValue{}
	for _, filename := range task.Reduce.IntermediateFilenames {
		file, _ := os.Open(filename)
		decoder := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", task.Reduce.Id)
	temp, _ := os.CreateTemp(".", oname)
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
		fmt.Fprintf(temp, "%v %v\n", kva[i].Key, output)
		i = j
	}

	os.Rename(temp.Name(), oname)
	defer temp.Close()
	fmt.Printf("doreduce finish\n")
	call("Coordinator.Finish", &FinishArgs{IsMap: false, Id: task.Reduce.Id}, &PlaceHolder{})
	return nil
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

	fmt.Println(err)
	return false
}
