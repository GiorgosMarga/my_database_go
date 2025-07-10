package btree

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"testing"
)

func TestSetHeader(t *testing.T) {
	node := make(BNode, BNODE_PAGE_SIZE)

	node.setHeader(BNODE_INTERNAL, 10)
	if BNODE_INTERNAL != node.getType() {
		log.Fatalf("Wrong Header expected %d, got %d\n", BNODE_INTERNAL, node.getType())
	}

	if node.getKeys() != 10 {
		log.Fatalf("Wrong Keys expected %d, got %d\n", 10, node.getKeys())
	}

	node.setHeader(BNODE_LEAF, 3)
	if BNODE_LEAF != node.getType() {
		log.Fatalf("Wrong Header expected %d, got %d\n", BNODE_LEAF, node.getType())
	}

	if node.getKeys() != 3 {
		log.Fatalf("Wrong Keys expected %d, got %d\n", 3, node.getKeys())
	}
}

func TestPtrs(t *testing.T) {
	ptrs := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	node := make(BNode, BNODE_PAGE_SIZE)
	node.setHeader(BNODE_INTERNAL, uint16(len(ptrs)))

	for i := range len(ptrs) {
		node.appendKV(uint16(i), ptrs[i], fmt.Appendf(nil, "%d", i), nil)
	}

	fmt.Println("Appended all")
	for i := range len(ptrs) {
		ptr := node.getPtr(uint16(i))
		key := node.getKey(uint16(i))

		if ptr != ptrs[i] {
			log.Fatalf("Expected ptr %d, got %d\n", ptrs[i], ptr)
		}
		if !bytes.Equal(key, fmt.Appendf(nil, "%d", i)) {
			log.Fatalf("Expected key %s, got %s\n", string(fmt.Appendf(nil, "%d", i)), string(key))
		}
	}

}

func TestLeafInsert(t *testing.T) {

	old := make(BNode, BNODE_PAGE_SIZE)
	numOfKeys := 8
	old.setHeader(BNODE_LEAF, uint16(numOfKeys)+1)
	old.appendKV(0, 0, nil, nil)
	for i := range numOfKeys {
		old.appendKV(uint16(i)+1, 0, fmt.Appendf(nil, "k_%d", i), fmt.Appendf(nil, "k_%d", i))
	}

	for i := range old.getKeys() {
		fmt.Printf("%s ", old.getKey(i))
	}
	fmt.Println()

	new := make(BNode, BNODE_PAGE_SIZE)

	idx := old.findKey(fmt.Appendf(nil, "k_%d", 9))
	fmt.Println(bytes.Compare([]byte("k_8"), []byte("k_9")))
	fmt.Println("Inserting: ", idx)
	leafInsert(old, new, idx+1, fmt.Appendf(nil, "inserted_%d", 3), fmt.Appendf(nil, "inserted_%d", 3))

	fmt.Println("New has", new.getKeys(), "keys")
	for i := range new.getKeys() {
		fmt.Println(string(new.getKey(uint16(i))))
	}
}

func TestGetBytes(t *testing.T) {
	node := make(BNode, BNODE_PAGE_SIZE)

	node.setHeader(2, 5)
	var totalBytes uint16 = HEADER_SIZE + PTRS_SIZE*5 + OFFSET_SIZE*5
	for i := range 5 {
		node.appendKV(uint16(i), 0, []byte("x"), []byte("y"))
		totalBytes += uint16(len([]byte("x"))) + uint16(len([]byte("y"))) + KLEN_SIZE + VLEN_SIZE
	}

	if totalBytes != node.getBytes() {
		log.Fatalf("Wrong size: expected %d got %d\n", totalBytes, node.getBytes())
	}

}

func TestSplitNodeTo2(t *testing.T) {
	node := make(BNode, BNODE_PAGE_SIZE)
	node.setHeader(BNODE_INTERNAL, 5)

	left := make(BNode, 2*BNODE_PAGE_SIZE)
	right := make(BNode, BNODE_PAGE_SIZE)

	for i := range 5 {
		ptr := uint64(rand.Intn(10_000_000))
		fmt.Println("Add:", ptr)
		node.appendKV(uint16(i), ptr, fmt.Appendf(nil, "%d", ptr), nil)
	}

	splitNodeTo2(node, left, right)

	for i := range left.getKeys() {
		fmt.Println(string(left.getKey(uint16(i))))
	}

	fmt.Println("Right", right.getKeys())
	for i := range right.getKeys() {
		fmt.Println(string(right.getKey(uint16(i))))
	}

}
