package kv

import (
	"bytes"
	"fmt"
	"log"
	"testing"
)

func TestInsert(t *testing.T) {
	kv := KV{}
	if err := kv.Init("test.db"); err != nil {
		log.Fatal(err)
	}
	k := fmt.Appendf(nil, "key_%d", 1)
	v := fmt.Appendf(nil, "val_%d", 1)
	if err := kv.Set(k, v); err != nil {
		log.Fatal(err)
	}

	val, err := kv.Get(k)
	if err != nil {
		log.Fatal(err)
	}
	if !bytes.Equal(v, val) {
		log.Fatalf("get: wrong value, expected: %s got %s\n", string(v), string(val))
	}
	if err := kv.Set(k, v); err != nil {
		log.Fatal(err)
	}
}
