package kv

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
)

func deleteDB(pathname string) error {
	return os.Remove(pathname)
}
func TestInsert(t *testing.T) {
	dbName := fmt.Sprintf("../db/test_%d.db", rand.Intn(1))
	defer deleteDB(dbName)
	kv := KV{}
	if err := kv.Init(dbName); err != nil {
		log.Fatal(err)
	}

	totalKeys := 1000

	keys := make([]int, totalKeys)
	for i := range totalKeys {
		randomKey := rand.Intn(1024)
		k := fmt.Appendf(nil, "k_%d", randomKey)
		v := fmt.Appendf(nil, "v_%d", randomKey)
		keys[i] = randomKey
		if err := kv.Insert(k, v); err != nil {
			deleteDB(dbName)
			log.Fatal(err)
		}
	}
	for _, key := range keys {
		k := fmt.Appendf(nil, "k_%d", key)
		expectedValue := fmt.Appendf(nil, "v_%d", key)
		v, err := kv.Get(k)
		if err != nil {
			deleteDB(dbName)

			log.Fatal(key, err)
		}
		if !bytes.Equal(v, expectedValue) {
			deleteDB(dbName)

			log.Fatalf("expxted %s got %s\n", string(expectedValue), string(v))
		}
	}
}

func TestGet(t *testing.T) {
	dbName := fmt.Sprintf("../db/test_%d.db", rand.Intn(1))
	kv := KV{}
	if err := kv.Init(dbName); err != nil {
		log.Fatal(err)
	}
	for i := range 10 {
		k := fmt.Appendf(nil, "key_%d", 10+i)
		expectedV := fmt.Appendf(nil, "val_%d", 10+i)
		v, err := kv.Get(k)
		if err != nil {
			log.Fatal(err)
		}
		if !bytes.Equal(v, expectedV) {
			log.Fatalf("expected: %s, got %s\n", string(expectedV), string(v))
		}
	}
}

func TestDelete(t *testing.T) {
	kv := KV{}

	if err := kv.Init("../db/test_delete.db"); err != nil {
		log.Fatal(err)
	}

	for i := range 1000 {
		k := fmt.Appendf(nil, "k_%d", i)
		v := fmt.Appendf(nil, "v_%d", i)
		if err := kv.Insert(k, v); err != nil {
			deleteDB("../db/test_delete.db")
			log.Fatal(err)
		}
	}

	for i := range 500 {
		k := fmt.Appendf(nil, "k_%d", i)
		if err := kv.Delete(k); err != nil {
			deleteDB("../db/test_delete.db")

			log.Fatal(err)
		}
	}

	for i := range 500 {
		k := fmt.Appendf(nil, "k_%d", 500)
		v, _ := kv.Get(k)
		if len(v) == 0 {
			deleteDB("../db/test_delete.db")
			log.Fatalf("%d was found\n", i)
		}
	}


}
