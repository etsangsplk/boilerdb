package prefix_tree

import (
	"container/heap"
	"container/list"
)

type Record struct {
	Key string
	//value string
	Score float32
}

func (r Record) GetScore() float32 {
	return r.Score
}

type Node struct {
	Char uint8
	//pos uint16
	Children []*Node
	Record   *Record
}

func newNode(pos uint16, char uint8) *Node {

	ret := new(Node)
	//ret.pos = pos
	ret.Char = char
	ret.Children = make([]*Node, 0)

	ret.Record = nil
	return ret

}

func (n *Node) _findChild(char uint8) (*Node, bool) {

	for i := 0; i < len(n.Children); i++ {

		//fmt.Printf("Comparing %c to %c\n", n.Children[i].Char, char)
		if n.Children[i].Char == char {
			//fmt.Println("Found!")

			return n.Children[i], true

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
			current.Children = append(current.Children, child)

			current = child

		}

	}

	//Create the new record and append it to the last node we've iterated/created

	if current.Record != nil {

		if score > current.Record.Score {
			current.Record.Score = score
		}
	} else {
		current.Record = new(Record)
		current.Record.Key = key
		current.Record.Score = score
	}
	//current.Record.value = value
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
			current.Children[key[pos]] = child

			current = child
		}

	}

	//Create the new record and append it to the last node we've iterated/created

	if current.Record != nil {
		current.Record.Score += amount

	} else {
		current.Record = new(Record)
		current.Record.Key = key
		current.Record.Score = amount

	}
	return current.Record.Score

}

///Exact find function, not prefixed
func (n *Node) get(key string) *Record {

	current := n
	for pos := 0; pos < len(key); pos++ {
		//find the child that corresponds to the next letter we want
		next, found := current._findChild(key[pos])
		//go down one node
		if found {

			//fmt.Printf("Following node %c pos %d\n", next.Char, next.pos)
			current = next

		} else { //this is a dead end! yield

			return nil
		}

	}

	//if we're here, it means we have a node for this key. if it has a record - we fond it. if not -we return nil
	return current.Record
}

type RecordList struct {
	records []string
}

// Perform a prefix search on the tree
// @param prefix the prefix to search
// @param withScores if set to true - we return the scores as part of the reply
// @return a list of Result structs

func (n *Node) prefixSearch(prefix string) ([]*Record, int) {

	max := 0
	num := 10

	current := n
	for pos := 0; pos < len(prefix); pos++ {

		//		fmt.Printf("Level %d node %c, len %d\n", pos, current.Char, len(current.Children))
		next, found := current._findChild(prefix[pos])
		if found {

			//fmt.Printf("Following node %c pos %d\n", next.Char, next.pos)
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

		if node.Record != nil {

			if node.Record.Score > lower || lower <= 0 {

				item := &Item{value: node.Record}
				heap.Push(&pq, item)

				if pq.Len() > num {
					lowestItem := heap.Pop(&pq).(*Item)
					lower = lowestItem.value.GetScore()
				}
			}

			max++
		}

		if len(node.Children) > 0 {
			for i := 0; i < len(node.Children); i++ {
				stack.PushFront(node.Children[i])
			}
		}

	}
	if num > max {
		num = max
	}

	ret := make([]*Record, num)

	for i := 0; i < num && len(pq) > 0; i++ {
		item := heap.Pop(&pq).(*Item)
		if item != nil {
			ret[num-i-1] = item.value.(*Record)
		}

	}

	return ret, max

}
