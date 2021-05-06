package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

const MapTaskFlag int = 10
const ReduceTaskFlag int = 20
const ShutdownFlag int = 30

type Task struct {
	File     string
	TaskKind int
	TaskId   string
}

type KeyValues struct {
	Key    string
	Values []string
}

type MapResult struct {
	TaskId string
	Items  []KeyValues
}

type ReduceResult struct {
	TaskId   string
	Filename string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

type RpcClient struct {
	Connection *rpc.Client
}

func (client *RpcClient) Call(funcName string, args interface{}, reply interface{}) bool {
	err := client.Connection.Call(funcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("call error", err)
	return false
}

//
// Close connection of RpcClient. Remember call when not use RpcClient object.
//
func (client *RpcClient) Close() {
	client.Connection.Close()
}

func MakeRpcClient() RpcClient {
	// c, err := rpc.Dial("tcp", ":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	return RpcClient{c}
}

// func StartRpcServerOf(obj interface{}) {
// 	rpc.Register(obj)
// 	l, e := net.Listen("tcp", ":1234")
// 	if e != nil {
// 		log.Fatal("listen error:", e)
// 	}

// 	for {
// 		conn, err := l.Accept()
// 		// 保存这个连接
// 		if err != nil {
// 			log.Fatal("accept error:", err)
// 		}
// 		// rpc 主要用来获得 map 和 reduce 的结果会比较方便
// 		go rpc.ServeConn(conn)
// 	}
// }

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
// func call(rpcname string, args interface{}, reply interface{}) bool {
// 	c, err := rpc.Dial("tcp", ":1234")
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}
// 	// sockname := coordinatorSock()
// 	// c, err := rpc.DialHTTP("unix", sockname)
// 	defer c.Close()

// 	err = c.Call(rpcname, args, reply)
// 	if err == nil {
// 		return true
// 	}

// 	fmt.Println(err)
// 	return false
// }
