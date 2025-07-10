package freelist

import (
	"fmt"

	"github.com/GiorgosMarga/my_db/btree"
)

type FreeList struct {
	Get    func(uint64) []byte
	Update func(uint64) []byte
	New    func([]byte) uint64

	HeadPage uint64
	HeadIdx  uint64

	TailPage uint64
	TailIdx  uint64

	MaxIdx uint64
}

func (fl *FreeList) getIdx(idx uint64) uint64 {
	return idx % MAX_PTRS
}
func (fl *FreeList) PopHead() uint64 {
	ptr, head := fl.pop()
	if head != 0 {
		// recycle node
		fl.PushTail(head)
	}
	return ptr
}
func (fl *FreeList) pop() (uint64, uint64) {
	if fl.HeadIdx == fl.MaxIdx {
		return 0, 0
	}
	next := LNode(fl.Get(fl.HeadPage)).getNext()
	ptr := LNode(fl.Get(fl.HeadPage)).getPtr(fl.getIdx(fl.HeadIdx))

	fl.HeadIdx++

	if fl.getIdx(fl.HeadIdx) == 0 {
		if next == 0 {
			panic("asda")
		}
		// this head page has no more available ptrs
		head := fl.HeadPage
		fl.HeadPage = next
		return ptr, head
	}
	return ptr, 0
}

func (fl *FreeList) PushTail(ptr uint64) {
	LNode(fl.Update(fl.TailPage)).setPtr(fl.getIdx(fl.TailIdx), ptr)

	fl.TailIdx++
	if fl.getIdx(fl.TailIdx) != 0 {
		// tail node has available space for more ptrs
		return
	}
	// recycle head page if exists or create a new page

	next, head := fl.pop()
	if next == 0 {
		// allocate new page
		next = fl.New(make([]byte, btree.BNODE_PAGE_SIZE))
	}

	LNode(fl.Update(fl.TailPage)).setNext(next)
	fl.TailPage = next
	// also add the head node if it's removed
	if head != 0 {
		fmt.Println("head was removed")
		LNode(fl.Update(fl.TailPage)).setPtr(0, head)
		fl.TailIdx++
	}

}

func (fl *FreeList) SetMaxIdx() {
	fl.MaxIdx = fl.TailIdx
}
