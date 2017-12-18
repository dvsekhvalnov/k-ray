package db

import (
	"bytes"
	"os"

	"github.com/dgraph-io/badger"
	. "github.com/dvsekhvalnov/k-ray/log"
)

type Operation func(txn *badger.Txn) error
type Lookup func(start, end, current []byte, count int) []byte

type DB struct {
	db *badger.DB
}

func (db *DB) Update(ops ...Operation) error {
	return db.db.Update(func(txn *badger.Txn) error {
		for _, op := range ops {
			err := op(txn)

			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (db *DB) Close() {
	Log.Println("Closing db.")
	db.db.Close()
}

func Open(dir string) (*DB, error) {

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		Log.Printf("Didn't find data dir:'%v'. Creating.\n", dir)
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			panic(err)
		}
	}

	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir

	db, err := badger.Open(opts)

	if err != nil {
		Log.Println("Unable to open embedded database at path:", dir, "error:", err)
		return nil, err
	}

	Log.Println("Succesully open database at:", dir)

	return &DB{db}, nil
}

func keyRangeScan(txn *badger.Txn,
	skip int,
	start, end []byte,
	prefix, ref Lookup,
	paginate func(keyIndex int, key []byte)) [][]byte {

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)

	result := make([][]byte, 0)

	keyIdx := 0

	for it.Seek(start); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()

		partitial := prefix(start, end, key, keyIdx)

		//scan till the end
		if bytes.Compare(partitial, end) >= 0 {
			break
		}

		//skip number of rows to support paging
		if keyIdx >= skip {
			result = append(result, CopyBytes(ref(start, end, key, keyIdx)))
		}

		//let caller chance to paginate or do whatever
		paginate(keyIdx, key)

		keyIdx++
	}

	return result
}

func fetchAll(txn *badger.Txn, refs [][]byte, limit int, key func(ref []byte) []byte, value func(blob []byte)) {
	for i := 0; i < len(refs) && i < limit; i++ {
		if item, err := txn.Get(key(refs[i])); err == nil {
			if blob, err := item.Value(); err == nil {
				value(blob)
			}
		}
	}
}
func paginate(search *SearchRequest, result *SearchResponse) func(keyIndex int, key []byte) {
	var limit int = search.Paging.Limit
	var startPage int = search.Page
	var pages []int = search.Paging.Pages

	return func(keyIndex int, key []byte) {
		if keyIndex%limit == 0 {

			page := (keyIndex / limit) + startPage

			if len(pages) == 0 || Contains(pages, page) {
				result.AddOffset(page, CopyBytes(key))
			}
		}
	}
}
