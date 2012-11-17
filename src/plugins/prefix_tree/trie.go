package prefix_tree

import (
	"camlistore.org/pkg/lru"
	"container/heap"
	"container/list"
//	"encoding/json"
	"fmt"
//	"io"
//	"runtime"
//	"log"
//	"os"
//	"strconv"
//	"time"
////	"unsafe"
//	"reflect"
)

type Record struct {
	key string
	//value string
	score float32
}

func (r Record) GetScore() float32 {
	return r.score
}

type Node struct {
	char uint8
	//pos uint16
	children []*Node
	record   *Record
}

func newNode(pos uint16, char uint8) *Node {

	ret := new(Node)
	//ret.pos = pos
	ret.char = char

	ret.children = make([]*Node, 0)
	ret.record = nil


	return ret

}

func (n *Node) _findChild(char uint8) (*Node, bool) {

	for i := 0; i < len(n.children); i++ {

		//fmt.Printf("Comparing %c to %c\n", n.children[i].char, char)
		if n.children[i].char == char {
			//fmt.Println("Found!")

			return n.children[i], true

		}
	}
	//fmt.Println("NOT FOUND!")
	return nil, false
}

///insert a new record into the index
func (n *Node) set(key string, score float32, value string) {

	current := n
	//find or create the node to put this record on 
	for pos := 0; pos < len(key); pos++ {

		next, found := current._findChild(key[pos])

		//we're iterating an existing node here
		if found {

			current = next

		} else { //nothing for this prefix - create a new node

			child := newNode(uint16(pos), key[pos])
			current.children = append(current.children, child)

			current = child

		}

	}

	//Create the new record and append it to the last node we've iterated/created

	if current.record != nil {

		if score > current.record.score {
			current.record.score = score
		}
	} else {
		current.record = new(Record)
		current.record.key = key
		current.record.score = score
	}
	//current.record.value = value
}

///insert a new record into the index
func (n *Node) increment(key string, amount float32) float32 {

	current := n
	//find or create the node to put this record on
	for pos := 0; pos < len(key); pos++ {

		next, found := current._findChild(key[pos])
		//we're iterating an existing node here
		if found {

			current = next

		} else { //nothing for this prefix - create a new node

			child := newNode(uint16(pos), key[pos])
			current.children[key[pos]] = child

			current = child
		}

	}

	//Create the new record and append it to the last node we've iterated/created

	if current.record != nil {
		current.record.score += amount

	} else {
		current.record = new(Record)
		current.record.key = key
		current.record.score = amount

	}
	return current.record.score

}

///Exact find function, not prefixed
func (n *Node) get(key string) *Record {

	current := n
	for pos := 0; pos < len(key); pos++ {
		//find the child that corresponds to the next letter we want
		next, found := current._findChild(key[pos])
		//go down one node
		if found {

			//fmt.Printf("Following node %c pos %d\n", next.char, next.pos)
			current = next

		} else { //this is a dead end! yield 

			return nil
		}

	}

	//if we're here, it means we have a node for this key. if it has a record - we fond it. if not -we return nil
	return current.record
}

type RecordList struct {
	records []string
}




func (n *Node) prefixSearch(prefix string, withScores bool) (*[]string, int) {

	max := 0
	num := 10

	current := n
	for pos := 0; pos < len(prefix); pos++ {

//		fmt.Printf("Level %d node %c, len %d\n", pos, current.char, len(current.children))
		next, found := current._findChild(prefix[pos])
		if found {

			//fmt.Printf("Following node %c pos %d\n", next.char, next.pos)
			current = next

		} else {


			//fmt.Printf("Could not find anything for prefix %s\n", prefix)
			return nil, 0
		}

	}

	stack := list.New()
	stack.PushBack(current)
	pq := make(PriorityQueue, 0, num+1)

	var lower float32 = 0
	for stack.Len() > 0 {

		back := stack.Remove(stack.Back())
		node, _ := back.(*Node)

		if node.record != nil {

			if node.record.score > lower {

				item := &Item{value: node.record}
				heap.Push(&pq, item)

				if pq.Len() > num {
					lowestItem := heap.Pop(&pq).(*Item)
					lower = lowestItem.value.GetScore()
				}
			}

			max++
		}

		if len(node.children) > 0 {
			for i := 0; i < len(node.children); i++ {
				stack.PushFront(node.children[i])
			}
		}

	}
	if num > max {
		num = max
	}

	capacity := num
	step := 1
	if withScores {
		step = 2
		capacity*=2
	}
	ret := make([]string, capacity)

	for i := 0; i < capacity && len(pq) > 0; i+= step {
		item := heap.Pop(&pq).(*Item)
		ret[capacity-i-step] = item.value.(*Record).key
		if withScores {
			ret[capacity - i -1] = fmt.Sprintf("%f", item.value.(*Record).score)
		}


	}

	return &ret, max
}
//
//func populate(fileName string) *Node {
//
//	root := new(Node)
//	root.children = make([]*Node, 0)
//	//root.pos = 0
//
//	r, _ := os.Open(fileName)
//
//	reader := csv.NewReader(r)
//
//	n := 0
//
//	for n < 10000000 {
//
//		row, err := reader.Read()
//		if err == nil {
//
//			score, _ := strconv.ParseFloat(row[1], 32)
//			root.set(row[0], float32(score), "")
//			n++
//
//			if n%100000 == 0 {
//				fmt.Printf("Read %d rows\n", n)
//				fmt.Println(row)
//			}
//
//		} else if err == io.EOF {
//			break
//		}
//
//	}
//
//	return root
//
//}
//
//func (r *Record) Pack(w io.Writer) {
//
//
//	msgpack.Pack(w, []byte(r.key))
//	msgpack.Pack(w, r.score)
//}
//
//func UnPackRecord(r io.Reader) (string, float32, error) {
//
//
//	value, _, err := msgpack.Unpack(r)
//	if err != nil {
//		return "", 0, err
//	}
//	b := value.Interface().([]byte)
//	str := string(b)
//
//	value, _, err = msgpack.Unpack(r)
//	if err != nil {
//		return "", 0, err
//	}
//
//
//	var score float32 = 0
//	switch value.Kind() {
//		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
//			ival := value.Uint()
//			score = *(*float32)(unsafe.Pointer(&ival))
//			break
//		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
//			ival := value.Int()
//			score = *(*float32)(unsafe.Pointer(&ival))
//			break
//	}
//
//
//	return str, score, nil
//}
//
//
//func (n *Node)SaveDump(fileName string) bool {
//
//
//	fp, err := os.Create(fileName)
//	if err != nil {
//		log.Panicf("Could not open file '%s' for writing. Err: %s", fileName, err)
//		return false
//	}
//
//	stack := list.New()
//	stack.PushBack(n)
//	total := 0
//	for stack.Len() > 0 {
//
//		back := stack.Remove(stack.Back())
//		node, _ := back.(*Node)
//
//		if node.record != nil {
//			node.record.Pack(fp)
//			total += 1
//		}
//
//		if len(node.children) > 0 {
//			for i := 0; i < len(node.children); i++ {
//				stack.PushFront(node.children[i])
//			}
//		}
//
//	}
//
//	fmt.Printf("Written %d nodes\n", total)
//	fp.Close()
//	return total > 0
//
//}
//
//
//func LoadDump(fileName string) *Node {
//
//	n := new(Node)
//	n.children = make([]*Node, 0)
//
//	fp, err := os.Open(fileName)
//
//	if err != nil{
//		log.Panicf("Could not open file for reading: %s", err)
//	}
//
//	total := 0
//	for true {
//
//		str, score, err := UnPackRecord(fp)
//		if err == nil {
//			n.set(str, score, "")
//			total++
//
//		} else if err == io.EOF {
//			break
//		}
//
//		if total % 100000 == 0 {
//			fmt.Printf("Loaded %d records\n", total)
//		}
//	}
//
//	return n
//}

var _Cache *lru.Cache = nil

type CachedResult struct {

	Records  *[]string
	Max int
}
func getFromCache(prefix string)  (*CachedResult, bool) {

	res, ok := _Cache.Get(prefix)


	if ok {


		ret := res.(*CachedResult)
		//log.Printf("Found from cache for prefix %s, %d results", prefix, ret.Max)
		return ret, true
	}
	return nil, false
}

func setCache(prefix string, records *[]string, max int) {

	_Cache.Add(prefix, &CachedResult{ records, max })

}

//var root *Node = nil



//type CompletionResponse struct {
//	Records  *[]string
//	Max      int
//	Prefix   string
//	Duration string
//}
//
//func CompleteHandler(w http.ResponseWriter, r *http.Request) {
//
//	prefix := r.FormValue("prefix")
//	st := time.Now()
//
//	cached, found := getFromCache(prefix)
//	var ret *[]string = nil
//	var max int = 0
//
//	if found {
//		ret = cached.Records
//		max = cached.Max
//	} else {
//		ret, max = root.prefixSearch(prefix)
//
//		setCache(prefix, ret, max)
//	}
//	et := time.Now()
//
//	rr := CompletionResponse{Max: max, Prefix: prefix, Records: ret, Duration: et.Sub(st).String()}
//	resp, err := json.Marshal(rr)
//	if err == nil {
//
//		fmt.Fprintf(w, "%s", resp)
//	}
//}
//
//
//func IncrementHandler(w http.ResponseWriter, r *http.Request) {
//
//	key := r.FormValue("key")
//	amount, err := strconv.ParseFloat(r.FormValue("amount"), 32)
//	rc := float32(-1)
//	if err == nil {
//		rc = root.increment(key, float32(amount))
//	} else {
//		log.Print("Error parsing amount")
//	}
//
//	resp, _ := json.Marshal(struct{ Score float32 }{Score: rc})
//	fmt.Fprintf(w, "%s", resp)
//
//}
//
//func Precache() {
//
//	_DoPrecache := func(p string) {
//		fmt.Printf("Precaching %s\n", p)
//		ret, max := root.prefixSearch(p)
//		if max > 0 {
//			setCache(p, ret, max)
//		}
//	}
//
//	alphabet := "abcdefghijklmonpqrstuvwxyz"
//
//	for x := range alphabet {
//
//
//		p := fmt.Sprintf("%c", alphabet[x])
//		_DoPrecache(p)
//		for y := range alphabet {
//
//			prx := fmt.Sprintf("%c%c", alphabet[x], alphabet[y])
//			_DoPrecache(prx)
//		}
//	}
//
//
//}
//
//func main() {
//
//
//	_Cache = lru.New(1000)
//
//	runtime.GOMAXPROCS(50)
//
//	//root = populate("/home/dvirsky/acf.dump")
//	//root.SaveDump("/tmp/ac.dump")
//
//	root = LoadDump("/tmp/ac.dump")
//	//fmt.Println("loaded ok!")
//	Precache()
//	http.HandleFunc("/complete", CompleteHandler)
//	 http.HandleFunc("/incr", IncrementHandler)
//	fmt.Println(os.Getwd())
//
//
//
//	http.Handle("/", http.FileServer(http.Dir("./html")))
////
//	log.Fatal(http.ListenAndServe(":8282", nil))
//
//}

//	
