package btree

import (
	"bytes"
	"fmt"
	"log"
	"math/rand/v2"
	"testing"
)

type MockDisk struct {
	pages map[uint64][]byte
}

func (d *MockDisk) Get(ptr uint64) []byte {
	if _, ok := d.pages[ptr]; !ok {
		panic(ptr)
	}
	return d.pages[ptr]
}

func (d *MockDisk) New(data []byte) uint64 {
	// ptr := uint64(len(d.pages)) + 1
	ptr := rand.Uint64()
	d.pages[ptr] = data
	return ptr
}

func (d *MockDisk) Del(ptr uint64) {
	delete(d.pages, ptr)
}

func TestInsert(m *testing.T) {
	disk := MockDisk{
		pages: make(map[uint64][]byte),
	}
	t := Btree{
		Get: disk.Get,
		New: disk.New,
		Del: disk.Del,
	}

	numOfKeys := 1000

	keys := make([]int, numOfKeys)

	for i := range numOfKeys {
		key := rand.IntN(1024)
		keys[i] = key
		k := fmt.Appendf(nil, "k_%d", key)
		v := fmt.Appendf(nil, "v_%d", key)
		if err := t.Insert(k, v); err != nil {
			log.Fatal(err)
		}
	}

	for _, key := range keys {
		k := fmt.Appendf(nil, "k_%d", key)
		expectedV := fmt.Appendf(nil, "v_%d", key)
		v, err := t.GetValue(k)
		if err != nil {
			log.Fatal(key, err)
		}
		if !bytes.Equal(v, expectedV) {
			log.Fatalf("Expected %s got %s\n", string(expectedV), string(v))
		}
	}

}
