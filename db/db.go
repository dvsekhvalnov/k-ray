package db

import (
	"github.com/dgraph-io/badger"
	. "github.com/dvsekhvalnov/k-ray/log"
	"os"
)

type DB struct {
	db *badger.DB
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
