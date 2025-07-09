package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"syscall"

	"github.com/GiorgosMarga/my_db/btree"
	"golang.org/x/sys/unix"
)

const DB_DIG = "MY_DB_SIG_012345"

type KV struct {
	filename string
	fd       int
	failed   bool

	pages struct {
		flushed uint64
		nappend uint64
		updated [][]byte
	}

	mmap struct {
		chunks [][]byte
		size   uint64
	}

	tree btree.Btree
}

func (kv *KV) Init(filename string) error {
	kv.filename = filename
	if err := kv.createFileSync(filename); err != nil {
		return nil
	}

	stat, err := os.Stat(kv.filename)
	if err != nil {
		return fmt.Errorf("stat err: %w", err)
	}

	// set tree

	kv.tree.Del = func(u uint64) {}
	kv.tree.Get = kv.readPage
	kv.tree.New = kv.appendPage

	// config mmap struct
	kv.mmap = struct {
		chunks [][]byte
		size   uint64
	}{
		chunks: make([][]byte, 0),
	}

	// config pages struct
	kv.pages = struct {
		flushed uint64
		nappend uint64
		updated [][]byte
	}{
		updated: make([][]byte, 0),
	}

	size := stat.Size()
	if err := kv.readRoot(int(size)); err != nil {
		return err
	}

	return nil
}

func (kv *KV) createFileSync(filename string) error {
	flags := os.O_RDONLY | syscall.O_DIRECTORY

	folderFd, err := syscall.Open(path.Dir(filename), flags, 0o644)
	if err != nil {
		return fmt.Errorf("open folder: %w", err)
	}
	defer syscall.Close(folderFd)

	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(folderFd, path.Base(filename), flags, 0o644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	// fsync directory
	if err := syscall.Fsync(folderFd); err != nil {
		_ = syscall.Close(fd)
		return fmt.Errorf("folder sync: %w", err)
	}

	kv.fd = fd
	return nil

}

func (kv *KV) Set(k, v []byte) error {
	prevMeta := kv.createMeta()
	if err := kv.tree.Insert(k, v); err != nil {
		return err // invalid k or v length
	}

	return kv.updateOrRevert(prevMeta)
}

func (kv *KV) Get(k []byte) ([]byte, error) {
	return kv.tree.GetValue(k)
}
func (kv *KV) loadMeta(meta []byte) {

	if !bytes.Equal(meta[:16], []byte(DB_DIG)) {
		panic("wrong sig")
	}

	kv.tree.Root = binary.LittleEndian.Uint64(meta[16:])
	fmt.Println(kv.tree.Root)

	kv.pages.flushed = binary.LittleEndian.Uint64(meta[24:])
	fmt.Println(kv.pages.flushed)

	// kv.freelist.HeadPage = binary.LittleEndian.Uint64(meta[32:])

	// kv.freelist.HeadSeq = binary.LittleEndian.Uint64(meta[40:])

	// kv.freelist.TailPage = binary.LittleEndian.Uint64(meta[48:])
	// kv.freelist.TailPage = binary.LittleEndian.Uint64(meta[56:])
}

func (kv *KV) createMeta() []byte {
	meta := make([]byte, 64)

	fmt.Println("sig size: ", len(DB_DIG))
	copy(meta[0:], []byte(DB_DIG))
	binary.LittleEndian.PutUint64(meta[16:], kv.tree.Root)

	binary.LittleEndian.PutUint64(meta[24:], kv.pages.flushed)

	// binary.LittleEndian.PutUint64(meta[32:], kv.freelist.HeadPage)
	// binary.LittleEndian.PutUint64(meta[40:], kv.freelist.HeadSeq)
	// binary.LittleEndian.PutUint64(meta[48:], kv.freelist.TailPage)
	// binary.LittleEndian.PutUint64(meta[56:], kv.freelist.TailSeq)
	return meta
}

func (kv *KV) extendMMap(size uint64) error {
	if size <= kv.mmap.size {
		return nil
	}

	alloc := max(kv.mmap.size, 64<<20)

	for size > kv.mmap.size+alloc {
		alloc *= 2
	}

	chunk, err := syscall.Mmap(kv.fd, int64(kv.mmap.size), int(alloc), unix.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	kv.mmap.chunks = append(kv.mmap.chunks, chunk)
	kv.mmap.size += alloc

	return nil
}

func (kv *KV) writePages() error {
	size := (kv.pages.nappend + kv.pages.flushed) * btree.BNODE_PAGE_SIZE

	if err := kv.extendMMap(size); err != nil {
		return err
	}

	offset := kv.pages.flushed * btree.BNODE_PAGE_SIZE

	if _, err := unix.Pwritev(kv.fd, kv.pages.updated, int64(offset)); err != nil {
		return fmt.Errorf("write pages: %w", err)
	}

	kv.pages.flushed += kv.pages.nappend
	kv.pages.updated = kv.pages.updated[:0]
	kv.pages.nappend = 0
	return nil
}

func (kv *KV) readPage(ptr uint64) []byte {
	start := uint64(0)
	fmt.Println("Reading page:", ptr)

	for _, chunk := range kv.mmap.chunks {
		end := uint64(len(chunk)/btree.BNODE_PAGE_SIZE) + start
		fmt.Println(end)
		if ptr < end {
			offset := ptr - start
			fmt.Println(offset)
			return chunk[offset:][:btree.BNODE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

func (kv *KV) appendPage(node []byte) uint64 {
	ptr := kv.pages.flushed + uint64(len(kv.pages.updated))
	kv.pages.updated = append(kv.pages.updated, node)
	return ptr
}

func (kv *KV) updateFile() error {
	// 1. write nodes
	if err := kv.writePages(); err != nil {
		return err
	}
	// 2. flush file to make sure nodes are written
	if err := syscall.Fsync(kv.fd); err != nil {
		return err
	}

	// 3. update root
	if err := kv.updateRoot(); err != nil {
		return err
	}

	return syscall.Fsync(kv.fd)
}

func (kv *KV) updateRoot() error {
	if _, err := unix.Pwrite(kv.fd, kv.createMeta(), 0); err != nil {
		return err
	}
	return nil
}

func (kv *KV) updateOrRevert(meta []byte) error {
	err := kv.updateFile()
	if err != nil {
		// revert
		kv.loadMeta(meta)
		kv.pages.nappend = 0
		kv.pages.updated = kv.pages.updated[:0]
	}
	return err
}

func (kv *KV) readRoot(filesize int) error {
	if filesize == 0 {
		kv.pages.flushed = 2 // meta + freelist dummy page
		return nil
	}

	// if file alreacy exists need to load meta page in mmap
	chunk, err := syscall.Mmap(kv.fd, 0, filesize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	kv.mmap.chunks = append(kv.mmap.chunks, chunk)
	kv.mmap.size += uint64(filesize)
	kv.loadMeta(kv.mmap.chunks[0])
	return nil
}
