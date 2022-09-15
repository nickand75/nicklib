// AVL Tree in Golang
package avltree

import (
	"log"
	"time"
)

const cNodeDelDelay = 5 * time.Second

type Key interface {
	Less(Key) bool
	Eq(Key) bool
}

type Node struct {
	tree    *BTree
	Data    Key
	Balance int
	Link    [2]*Node
	delTime int64
	used    byte
}

func opp(dir int) int {
	return 1 - dir
}

func (root *Node) find(key Key) Key {

	if root.Data.Eq(key) {
		return root.Data
	}

	dir := 0
	if root.Data.Less(key) {
		dir = 1
	}
	if root.Link[dir] == nil {
		return nil
	} else {
		return root.Link[dir].find(key)
	}
}

func delNode(tree *BTree, node *Node) {
	node.delTime = time.Now().Add(cNodeDelDelay).Unix()
	node.used = 0
	select {
	case tree.delNodeQueue <- node:
	default:
		break
	}
}

// single rotation
func (root *Node) single(tree *BTree, dir int) *Node {

	// make copy of node
	cow := tree.getNewNode()
	*cow = *root

	// change copy...
	save := cow.Link[opp(dir)]
	cow.Link[opp(dir)] = save.Link[dir]
	save.Link[dir] = cow

	// mark to delete and replace node pointer
	delNode(tree, root)
	root = cow

	/*save := root.Link[opp(dir)]
	root.Link[opp(dir)] = save.Link[dir]
	save.Link[dir] = root*/

	return save
}

// double rotation
func (root *Node) double(tree *BTree, dir int) *Node {

	// make copy of two nodes
	cow := tree.getNewNode()
	*cow = *root

	cowNext := tree.getNewNode()
	cowNext = cow.Link[opp(dir)]

	// change nodes..
	save := cowNext.Link[dir]

	cowNext.Link[dir] = save.Link[opp(dir)]
	save.Link[opp(dir)] = cowNext
	cow.Link[opp(dir)] = save

	save = cowNext
	cowNext = save.Link[dir]
	save.Link[dir] = cow

	// mark to delete and replace node pointer
	delNode(tree, root)
	delNode(tree, cowNext)
	root = cow

	/*save := root.Link[opp(dir)].Link[dir]

	root.Link[opp(dir)].Link[dir] = save.Link[opp(dir)]
	save.Link[opp(dir)] = root.Link[opp(dir)]
	root.Link[opp(dir)] = save

	save = root.Link[opp(dir)]
	root.Link[opp(dir)] = save.Link[dir]
	save.Link[dir] = root*/
	return save
}

// adjust valance factors after double rotation
func (root *Node) adjustBalance(dir, bal int) {
	n := root.Link[dir]
	nn := n.Link[opp(dir)]
	switch nn.Balance {
	case 0:
		root.Balance = 0
		n.Balance = 0
	case bal:
		root.Balance = -bal
		n.Balance = 0
	default:
		root.Balance = 0
		n.Balance = bal
	}
	nn.Balance = 0
}

func (root *Node) insertBalance(tree *BTree, dir int) *Node {
	n := root.Link[dir]
	bal := 2*dir - 1
	if n.Balance == bal {
		root.Balance = 0
		n.Balance = 0
		return root.single(tree, opp(dir))
	}
	root.adjustBalance(dir, bal)
	return root.double(tree, opp(dir))
}

func (root *Node) insertR(tree *BTree, data Key) (*Node, bool, bool) {
	if root == nil {
		n := tree.getNewNode()
		n.Data = data
		return n, false, true
	}

	if root.Data.Eq(data) {
		return root, true, false
	}

	dir := 0
	if root.Data.Less(data) {
		dir = 1
	}
	var (
		done     bool
		inserted bool
	)
	root.Link[dir], done, inserted = root.Link[dir].insertR(tree, data)
	if done {
		return root, true, inserted
	}
	root.Balance += 2*dir - 1
	switch root.Balance {
	case 0:
		return root, true, inserted
	case 1, -1:
		return root, false, inserted
	}
	return root.insertBalance(tree, dir), true, inserted
}

func (root *Node) removeBalance(tree *BTree, dir int) (*Node, bool) {
	n := root.Link[opp(dir)]
	bal := 2*dir - 1
	switch n.Balance {
	case -bal:
		root.Balance = 0
		n.Balance = 0
		return root.single(tree, dir), false
	case bal:
		root.adjustBalance(opp(dir), -bal)
		return root.double(tree, dir), false
	}
	root.Balance = -bal
	n.Balance = bal
	return root.single(tree, dir), true
}

func (root *Node) removeR(tree *BTree, data Key) (*Node, bool) {
	if root == nil {
		return nil, false
	}
	if root.Data.Eq(data) {
		switch {
		case root.Link[0] == nil:
			return root.Link[1], false
		case root.Link[1] == nil:
			return root.Link[0], false
		}
		heir := root.Link[0]
		for heir.Link[1] != nil {
			heir = heir.Link[1]
		}
		root.Data = heir.Data
		data = heir.Data
	}
	dir := 0
	if root.Data.Less(data) {
		dir = 1
	}
	var done bool
	root.Link[dir], done = root.Link[dir].removeR(tree, data)
	if done {
		return root, true
	}
	root.Balance += 1 - 2*dir
	switch root.Balance {
	case 1, -1:
		return root, true
	case 0:
		return root, false
	}
	return root.removeBalance(tree, dir)
}

type BTree struct {
	cacheSize    int
	nodeCache    [][]Node
	freeCache    []*Node
	insKeyQueue  chan Key
	delKeyQueue  chan Key
	delNodeQueue chan *Node
	root         *Node
	Min          Key
	Max          Key
}

func NewBTree(cacheSize int, insQueueSize int, delQueueSize int) (t *BTree) {
	t = &BTree{
		cacheSize:    cacheSize,
		nodeCache:    make([][]Node, 0),
		freeCache:    make([]*Node, cacheSize),
		insKeyQueue:  make(chan Key, insQueueSize),
		delKeyQueue:  make(chan Key, delQueueSize),
		delNodeQueue: make(chan *Node, cacheSize/4),
	}

	t.nodeCache[0] = make([]Node, cacheSize)
	for i := 0; i < cacheSize; i++ {
		t.freeCache[i] = &t.nodeCache[0][i]
	}

	go t.work()

	return t
}

func (t *BTree) getNewNode() (res *Node) {

	// check if cache is empty and grow up
	if len(t.freeCache) == 0 {

		k := len(t.nodeCache)
		t.nodeCache = append(t.nodeCache, make([]Node, t.cacheSize/4))

		// calc total cache size
		cacheSize := 0
		for i := 0; i < len(t.nodeCache); i++ {
			cacheSize += len(t.nodeCache[i])
		}

		t.freeCache = make([]*Node, 0, cacheSize)
		t.freeCache = t.freeCache[:len(t.nodeCache[k])]

		for i := 0; i < len(t.nodeCache[k]); i++ {
			t.freeCache[i] = &t.nodeCache[k][i]
		}
	}

	res = t.freeCache[len(t.freeCache)-1]
	t.freeCache = t.freeCache[:len(t.freeCache)-1]

	res.tree = t
	res.delTime = 0
	res.used = 1

	return
}

func (t *BTree) work() {

	select {
	case req := <-t.insKeyQueue:
		t.insert(req)

	case req := <-t.delKeyQueue:
		t.remove(req)

	case n := <-t.delNodeQueue:
		if n.delTime > 0 && n.delTime <= time.Now().Unix() {
			t.freeCache = append(t.freeCache, n)
		}
	}

}

func (t *BTree) insert(data Key) {
	if t.root == nil {
		t.root = t.getNewNode()
		t.root.Data = data
		t.Min = data
		t.Max = data
		return
	}

	_, _ := t.root.insertR(t, data)
	if data.Less(t.Min) {
		t.Min = data
	}

	if t.Max.Less(data) {
		t.Max = data
	}
}

func (t *BTree) Insert(data Key) {

	select {
	case t.insKeyQueue <- data:

	default:
		log.Println("Cannot send to insert Queue!")
	}
}

func (t *BTree) remove(key Key) {

	if t.root == nil {
		return
	}

	t.root.removeR(t, key)
}

func (t *BTree) Remove(data Key) {
	select {
	case t.delKeyQueue <- data:

	default:
		log.Println("Cannot send to insert Queue!")
	}
}

func (t *BTree) Get(key Key) Key {
	if key.Less(t.Min) || t.Max.Less(key) {
		return nil
	}
	return t.root.find(key)
}

type Iterator func(i Key) bool

// iterate...
func (t *BTree) Iterate(iter Iterator) {

	for i := 0; i < len(t.nodeCache); i++ {
		for j := 0; j < len(t.nodeCache[i]); j++ {
			if t.nodeCache[i][j].used == 1 && t.nodeCache[i][j].Data != nil {
				if !iter(t.nodeCache[i][j].Data) {
					return
				}
			}
		}
	}
}
