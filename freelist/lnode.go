package freelist

import (
	"encoding/binary"

	"github.com/GiorgosMarga/my_db/btree"
)

type LNode []byte

// NEXT PTR | PAGE PTRS
// 8B          max_ptrs * 8b

const (
	PTR_SIZE      = 8
	NEXT_PTR_SIZE = 8
	MAX_PTRS      = (btree.BNODE_PAGE_SIZE - NEXT_PTR_SIZE) / PTR_SIZE
)

func (n LNode) setNext(ptr uint64) {
	binary.LittleEndian.PutUint64(n[0:], ptr)
}

func (n LNode) getNext() uint64 {
	return binary.LittleEndian.Uint64(n[0:])
}

func (n LNode) setPtr(idx, ptr uint64) {
	pos := NEXT_PTR_SIZE + idx*PTR_SIZE
	binary.LittleEndian.PutUint64(n[pos:], ptr)
}

func (n LNode) getPtr(idx uint64) uint64 {
	pos := NEXT_PTR_SIZE + idx*PTR_SIZE
	return binary.LittleEndian.Uint64(n[pos:])
}
