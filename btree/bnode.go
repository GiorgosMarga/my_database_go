package btree

import (
	"bytes"
	"encoding/binary"
)

type BNode []byte

const (
	BNODE_INTERNAL = iota
	BNODE_LEAF
)

const (
	HEADER_SIZE = 4
	PTRS_SIZE   = 8
	OFFSET_SIZE = 2
	KLEN_SIZE   = 2
	VLEN_SIZE   = 2

	BNODE_PAGE_SIZE = 4096
)

// HEADER | PTRS 		| OFFSETS 		| KV Pairs
// 4b       nkeys * 8n    nkeys * 2b      2b + 2b + (k+v)b
func (n BNode) setHeader(nodeType uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(n[0:], nodeType)
	binary.LittleEndian.PutUint16(n[2:], nkeys)
}

func (n BNode) getType() uint16 {
	return binary.LittleEndian.Uint16(n[0:])
}

func (n BNode) getKeys() uint16 {
	return binary.LittleEndian.Uint16(n[2:])
}

func (n BNode) setPtr(idx uint16, ptr uint64) {
	pos := HEADER_SIZE + PTRS_SIZE*idx
	binary.LittleEndian.PutUint64(n[pos:], ptr)
}

func (n BNode) getPtr(idx uint16) uint64 {
	pos := HEADER_SIZE + PTRS_SIZE*idx
	return binary.LittleEndian.Uint64(n[pos:])
}

func (n BNode) setOffset(idx uint16, offset uint16) {
	pos := HEADER_SIZE + PTRS_SIZE*n.getKeys() + OFFSET_SIZE*idx
	binary.LittleEndian.PutUint16(n[pos:], offset)
}

func (n BNode) getOffset(idx uint16) uint16 {
	pos := HEADER_SIZE + PTRS_SIZE*n.getKeys() + OFFSET_SIZE*idx
	return binary.LittleEndian.Uint16(n[pos:])
}

func (n BNode) getKVPos(idx uint16) uint16 {
	pos := HEADER_SIZE + PTRS_SIZE*n.getKeys() + OFFSET_SIZE*n.getKeys()
	// each offeset tells us where
	// each idx ends, so idx 0 shows where idx 0 ends which means where idx 1 should start
	if idx > 0 {
		pos += n.getOffset(idx - 1)
	}
	return pos
}

func (n BNode) getKey(idx uint16) []byte {
	pos := n.getKVPos(idx)

	klen := binary.LittleEndian.Uint16(n[pos:])
	return n[pos+KLEN_SIZE+VLEN_SIZE:][:klen]
}

func (n BNode) getVal(idx uint16) []byte {
	pos := n.getKVPos(idx)

	klen := binary.LittleEndian.Uint16(n[pos:])
	vlen := binary.LittleEndian.Uint16(n[pos+KLEN_SIZE:])
	return n[pos+KLEN_SIZE+VLEN_SIZE+uint16(klen):][:vlen]
}

func (n BNode) appendKV(idx uint16, ptr uint64, k, v []byte) {
	n.setPtr(idx, ptr) // this does nothing in case the node is a LEAF node

	pos := n.getKVPos(idx)

	binary.LittleEndian.PutUint16(n[pos:], uint16(len(k)))
	binary.LittleEndian.PutUint16(n[pos+KLEN_SIZE:], uint16(len(v)))
	copy(n[pos+KLEN_SIZE+VLEN_SIZE:], k)
	copy(n[pos+KLEN_SIZE+VLEN_SIZE+KLEN_SIZE+uint16(len(k)):], v)

	// offset starts counting from where the offset section ends, so it needs to add the previous offset every time
	offset := VLEN_SIZE + KLEN_SIZE + len(k) + len(v) + int(n.getOffset(idx-1))

	n.setOffset(idx, uint16(offset))

}

func (n BNode) findKey(k []byte) uint16 {
	var i uint16 = 0

	for i = range n.getKeys() {
		key := n.getKey(uint16(i))
		cmp := bytes.Compare(key, k)
		if cmp == 0 {
			return i
		}
		if cmp > 0 {
			return i - 1
		}
	}
	return i - 1
}

func copynKV(src BNode, srcIdx uint16, dst BNode, dstIdx uint16, n uint16) {
	for i := range n {

		k := src.getKey(srcIdx + uint16(i))
		v := src.getVal(srcIdx + uint16(i))
		dst.appendKV(dstIdx+uint16(i), src.getPtr(srcIdx+uint16(i)), k, v)
	}
}

func leafUpdate(old, new BNode, idx uint16, k, v []byte) {
	new.setHeader(BNODE_LEAF, old.getKeys())
	copynKV(old, 0, new, 0, idx)
	new.appendKV(idx, 0, k, v)
	copynKV(old, idx+1, new, idx+1, old.getKeys()-idx-1)
}

func leafInsert(old, new BNode, idx uint16, k, v []byte) {
	new.setHeader(BNODE_LEAF, old.getKeys()+1)
	copynKV(old, 0, new, 0, idx)
	new.appendKV(idx, 0, k, v)
	copynKV(old, idx, new, idx+1, old.getKeys()-idx)
}

func (n BNode) getBytes() uint16 {
	return n.getKVPos(n.getKeys())
}

func splitNodeTo2(node BNode, newLeft, newRight BNode) {
	idx := node.getKeys() / 2

	// get KVPos gives us the bytes neede up to idx
	for node.getKVPos(idx) > BNODE_PAGE_SIZE {
		idx--
	}

	for node.getBytes()-node.getKVPos(idx)+HEADER_SIZE > BNODE_PAGE_SIZE {
		idx++
	}

	newLeft.setHeader(node.getType(), idx)
	newRight.setHeader(node.getType(), node.getKeys()-idx)
	copynKV(node, 0, newLeft, 0, idx)
	copynKV(node, idx, newRight, 0, node.getKeys()-idx)

}

func splitNode(node BNode) (uint16, [3]BNode) {

	if node.getBytes() <= BNODE_PAGE_SIZE {
		return 0, [3]BNode{node[:BNODE_PAGE_SIZE]} // no split
	}

	left := make(BNode, 2*BNODE_PAGE_SIZE) // 2 * because it might need to be split again
	right := make(BNode, BNODE_PAGE_SIZE)
	splitNodeTo2(node, left, right)

	if left.getBytes() <= BNODE_PAGE_SIZE {
		return 2, [3]BNode{left[:BNODE_PAGE_SIZE], right}
	}

	leftLeft := make(BNode, BNODE_PAGE_SIZE)
	rightLeft := make(BNode, BNODE_PAGE_SIZE)

	splitNodeTo2(left, leftLeft, rightLeft)

	return 3, [3]BNode{leftLeft, rightLeft, right}

}

func replace2Ptrs(new, old BNode, newPtr uint64, idx uint16, key []byte) {
	new.setHeader(old.getType(), old.getKeys())

	copynKV(old, 0, new, 0, idx)
	new.appendKV(idx, newPtr, key, nil)
	copynKV(old, idx+2, new, idx+1, old.getKeys()-idx-2)

}

func merge2Nodes(left, right, merged BNode) {
	merged.setHeader(left.getType(), left.getKeys()+right.getKeys())

	copynKV(left, 0, merged, 0, left.getKeys())
	copynKV(right, 0, merged, left.getKeys(), right.getKeys())

}
