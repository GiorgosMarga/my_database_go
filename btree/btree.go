package btree

import (
	"bytes"
	"errors"
	"fmt"
)

const (
	BTREE_MAX_KEY_SIZE = 1024
	BTREE_MAX_VAL_SIZE = 1024
)

type Btree struct {
	Root uint64
	Get  func(uint64) []byte
	New  func([]byte) uint64
	Del  func(uint64)
}

func (t *Btree) GetValue(k []byte) ([]byte, error) {
	if t.Root == 0 {
		return nil, fmt.Errorf("empty tree")
	}

	val := t.getValue(t.Get(t.Root), k)
	if len(val) == 0 {
		return val, fmt.Errorf("key doesnt exist")
	}
	return val, nil
}

func (t *Btree) getValue(node BNode, k []byte) []byte {
	ptr := node.findKey(k)

	switch node.getType() {
	case BNODE_LEAF:
		if !bytes.Equal(node.getKey(ptr), k) {
			return []byte{} // key doesnt exist
		}
		return node.getVal(ptr)
	case BNODE_INTERNAL:
		childNode := node.getPtr(ptr)
		return t.getValue(t.Get(childNode), k)
	}
	panic("invalid key")
}
func (t *Btree) Insert(k, v []byte) error {
	if len(k) > BTREE_MAX_KEY_SIZE {
		return errors.New("key is too big")
	}

	if len(v) > BTREE_MAX_VAL_SIZE {
		return errors.New("val is too big")
	}

	if t.Root == 0 {
		New := make(BNode, BNODE_PAGE_SIZE)
		New.setHeader(BNODE_LEAF, 2)
		New.appendKV(0, 0, nil, nil)
		New.appendKV(1, 0, k, v)
		t.Root = t.New(New)
		return nil
	}

	node := t.insertNode(t.Get(t.Root), k, v)

	nsplit, splitted := splitNode(node)

	t.Del(t.Root)
	if nsplit == 0 {
		t.Root = t.New(splitted[0])
	} else {
		newRoot := make(BNode, BNODE_PAGE_SIZE)
		newRoot.setHeader(BNODE_INTERNAL, nsplit)
		for i, newSplitted := range splitted {
			newRoot.appendKV(uint16(i), t.New(newSplitted), newSplitted.getKey(0), nil)
		}
		t.Root = t.New(newRoot)
	}

	return nil
}

func (t *Btree) insertNode(node BNode, k, v []byte) BNode {
	idx := node.findKey(k)
	fmt.Println(idx)
	New := make(BNode, 2*BNODE_PAGE_SIZE)

	switch node.getType() {
	case BNODE_LEAF:
		if bytes.Equal(node.getKey(idx), k) {
			leafUpdate(node, New, idx, k, v)
		} else {
			leafInsert(node, New, idx+1, k, v)
		}
	case BNODE_INTERNAL:
		childPtr := node.getPtr(idx)
		updated := t.insertNode(t.Get(childPtr), k, v)
		t.Del(childPtr)

		nsplit, splitted := splitNode(updated)

		if nsplit > 1 {
			copynKV(node, 0, New, 0, idx)
			for i, newSplitted := range splitted {
				New.appendKV(idx+uint16(i), t.New(newSplitted), newSplitted.getKey(0), nil)
			}
			copynKV(node, idx+1, New, idx+nsplit, node.getKeys()-idx-1)
		}

	}
	return New
}

func (t *Btree) Delete(k []byte) (bool, error) {
	if t.Root == 0 {
		return false, nil
	}

	newRoot := t.deleteNode(t.Get(t.Root), k)

	t.Del(t.Root)
	t.Root = t.New(newRoot)
	return true, nil
}

func (t *Btree) deleteNode(node BNode, k []byte) BNode {

	idx := node.findKey(k)

	New := make(BNode, BNODE_PAGE_SIZE)

	switch node.getType() {
	case BNODE_LEAF:
		if !bytes.Equal(k, node.getKey(idx)) {
			return BNode{} // not exists
		}
		New.setHeader(BNODE_LEAF, node.getKeys()-1)
		copynKV(node, 0, New, 0, idx)
		copynKV(node, idx+1, New, idx, node.getKeys()-idx-1)
	case BNODE_INTERNAL:
		childPtr := node.getPtr(idx)
		child := t.deleteNode(t.Get(childPtr), k) // contains the updated node (keys is removed if exists)

		mergeDirection, sibling := t.shouldMerge(node, child, idx)

		switch {
		case mergeDirection == -1: // merge with left sibling
			merged := make(BNode, BNODE_PAGE_SIZE)
			merge2Nodes(sibling, child, merged)
			t.Del(node.getPtr(idx - 1))
			replace2Ptrs(New, node, t.New(merged), idx-1, merged.getKey(0))
		case mergeDirection == 1: // merge with left sibling
			merged := make(BNode, BNODE_PAGE_SIZE)
			merge2Nodes(child, sibling, merged)
			t.Del(node.getPtr(idx))
			replace2Ptrs(New, node, t.New(merged), idx, merged.getKey(0))
		case mergeDirection == 0 && child.getKeys() == 0:
			New.setHeader(BNODE_INTERNAL, 0)
		case mergeDirection == 0 && child.getKeys() > 0:
			New.setHeader(BNODE_INTERNAL, node.getKeys())
			copynKV(node, 0, New, 0, idx)
			New.appendKV(idx, t.New(child), child.getKey(0), nil)
			copynKV(node, idx+1, New, idx+1, node.getKeys()-idx-1)
		}

		t.Del(childPtr)

	}
	return New
}

func (t *Btree) shouldMerge(parent, updated BNode, idx uint16) (int, BNode) {
	if updated.getBytes() < BNODE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx-1 > 0 {
		leftSibling := BNode(t.Get(parent.getPtr(idx - 1)))
		if leftSibling.getBytes()+updated.getBytes() <= BNODE_PAGE_SIZE {
			return -1, leftSibling
		}
	}

	if idx+1 < parent.getKeys() {
		rightSibling := BNode(t.Get(parent.getPtr(idx + 1)))
		if rightSibling.getBytes()+updated.getBytes() <= BNODE_PAGE_SIZE {
			return 1, rightSibling
		}
	}

	return 0, BNode{}

}
