package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// // main/mrworker.go calls this function.
// func Worker(mapf func(string, string) []KeyValue,
// 	reducef func(string, []string) string) {

// 	// Your worker implementation here.
// 	println("worker function")
// 	// uncomment to send the Example RPC to the coordinator.
// 	// CallExample()
// 	Register()
// 	// 异步执行这段代码
// 	while(true){
// 		task := ReqeustTask()
// 		err = doTask(task)
// 		if(err != nil){
// 			break
// 		}
// 	}

// 	while(true){
// 		err = HeartBeat()
// 		if(err != nil){
// 			break
// 		}
// 	}

// }

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 使用 WaitGroup 来等待所有协程完成
	log.Println("call example start")
	CallExample()
	log.Println("call example end")
	var wg sync.WaitGroup
	log.Println("register begin ")
	// 注册
	workerId, err := WorkerDiscover()
	log.Println("register finish, workerId: %v\n", workerId)

	if err != nil {
		return
	}

	// 异步执行这段代码
	go func() {
		defer wg.Done()
		for {
			task, err := RequestTask(workerId)
			if err != nil {
				break
			}
			if err := doTask(&task, mapf, reducef); err != nil {
				break
			}
		}
	}()

	// 心跳
	go func() {
		defer wg.Done()
		for {
			if err := HeartBeat(workerId); err != nil {
				break
			}
			time.Sleep(10 * time.Second) // 每秒执行一次心跳
		}
	}()

	// 等待所有协程完成
	wg.Add(2)
	wg.Wait()
}

func doTask(task *Task,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {
	var err error
	if task.TaskType == MAP {
		err = doMapf(task, mapf)
	} else {
		err = doReducef(task, reducef)
	}
	return err
}

func doMapf(task *Task, mapf func(string, string) []KeyValue) error {
	filename := task.InputLocation
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
	// 排序
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	outputFile, err := os.Create(task.OutputLocation)
	if err != nil {
		return err
	}
	for _, kv := range kva {
		_, err := outputFile.WriteString(kv.Key + " " + kv.Value + "\n")
		if err != nil {
			return err
		}
	}
	outputFile.Close()
	return nil
}

func ByKey(kva []KeyValue) {
	panic("unimplemented")
}

func readKeysValuePairsFromFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v: %v", filename, err)
	}
	defer file.Close()
	// 创建一个 Scanner 用于逐行读取文件内容
	scanner := bufio.NewScanner(file)

	// 逐行读取文件内容并解析为键值对
	pairs := []KeyValue{}
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) != 2 {
			fmt.Printf("Invalid line: %v\n", line)
			continue
		}
		key := parts[0]
		value := parts[1]

		// 在这里你可以使用 key 和 value 进行自己的逻辑处理
		// 例如，将它们存储到一个数据结构中或进行其他操作
		keyValue := KeyValue{
			Key:   key,
			Value: value,
		}
		pairs = append(pairs, keyValue)
	}

	// 检查是否有扫描错误
	if err := scanner.Err(); err != nil {
		log.Fatalf("scan error: %v", err)
	}
	if err != nil {
		return []KeyValue{}, err
	}
	return pairs, nil
}

func doReducef(task *Task, reducef func(string, []string) string) error {
	intermediate, err := readKeysValuePairsFromFile(task.InputLocation)
	if err != nil {
		return err
	}
	oname := task.OutputLocation
	ofile, _ := os.Create(oname)

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

	ofile.Close()
	return nil
}

func RequestTask(workerId int32) (Task, error) {
	args := RequestTaskArgs{WorkerId: workerId}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		return Task{}, errors.New("rpc call failed")
	}
	if !reply.Success {
		return Task{}, errors.New("request task failed")
	}

	return reply.Task, nil
}

func HeartBeat(workerId int32) error {

	args := HeartBeatArgs{WorkerId: workerId}
	reply := HeartBeatReply{}

	ok := call("Coordinator.Example", &args, &reply)
	if !ok {
		return errors.New("rpc call failed")
	}
	if !reply.Success {
		return errors.New("heart beat failed")
	}
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

func WorkerDiscover() (int32, error) {
	args := WorkerDiscoverArgs{}
	reply := WorkerDiscoverReply{}
	log.Println("register call start")
	ok := call("Coordinator.WorkerDiscover", &args, &reply)
	log.Println("register call end")
	if ok {
		// reply.Y should be 100.
		log.Printf("reply %v\n", reply)
		return reply.WorkerId, nil
	} else {
		log.Printf("register call failed!\n")
		return 0, errors.New("register failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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
