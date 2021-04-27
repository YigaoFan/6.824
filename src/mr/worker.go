package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
)

// 凡是个类型都要像下面这样声明一套可以排序的接口，很麻烦吧？
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
// 不同 key 就分到不同的 NReduce 份下去了，然后针对每一份里的 key 进行 reduce
// 这样分有什么道理吗？这个份最终还是以文件存储吗？
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func DivideIntoItems(pairs []KeyValue) []Item {
	items := []Item{{Key: pairs[0].Key, Values: []string{pairs[0].Value}}}
	for _, p := range pairs {
		last := &items[len(items)-1]
		if p.Key == last.Key {
			last.Values = append(last.Values, p.Value)
		} else {
			i := Item{Key: p.Key, Values: []string{p.Value}}
			items = append(items, i)
		}
	}
	return items
}

// TODO 要测试下
func readFrom(reader *bufio.Reader) (bool, string, []string) {
	line, e := reader.ReadString('\n')
	words := strings.Fields(line) // 这里会在 words 里保留换行符吗？
	return e == io.EOF, words[0], words[1:]
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var task Task
	for {
		// 对端的 server 如果退出了，下面这个会有什么反应
		call("Coordinator.RequestTask", nil, &task)

		switch task.TaskKind {
		case MapTaskFlag:
			intermediate := mapf(task.File, ReadFileToString(task.File))
			sort.Sort(ByKey(intermediate))
			r := MapResult{TaskId: task.TaskId, Items: DivideIntoItems(intermediate)}
			call("Coordinator.UploadMapResult", r, nil)

		case ReduceTaskFlag:
			filename := fmt.Sprint("reduce-result-", task.TaskId)
			f, _ := os.Create(filename)
			defer f.Close()
			reader := bufio.NewReader(f)

			for {
				end, k, vs := readFrom(reader)
				if end {
					break
				}
				v := reducef(k, vs)
				f.WriteString(v)
			}

			result := ReduceResult{TaskId: task.TaskId, Filename: filename}
			call("Coordinator.UploadReduceResult", result, nil)
		case ShutdownFlag:
			fallthrough
		default:
			return
		}
	}
}

func ReadFileToString(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	// args := ExampleArgs{}

	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	// reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	// fmt.Printf("reply.Y %v\n", reply.Y)
}
