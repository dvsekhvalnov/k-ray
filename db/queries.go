package db

import (
	"github.com/dgraph-io/badger"
	. "github.com/dvsekhvalnov/k-ray/log"
)

type Datastore interface {
	Close()

	Save(msg *Message) error
	FindMessage(topic string, partition int32, offset int64) (*Message, error)

	SaveOffset(topic string, partition int32, offset int64) error
	FindOffset(topic string, partition int32) (int64, error)
}

func (db *DB) SaveOffset(topic string, partition int32, offset int64) error {

	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(
			Key(Offsets, []byte(topic), Int32ToBytes(partition)),
			Int64ToBytes(offset),
			0x00)
	})

	if err != nil {
		Log.Println("There were error during saving offset. Will retry. Err=", err)
	}

	return err
}

func (db *DB) FindOffset(topic string, partition int32) (int64, error) {

	var blob []byte

	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(Key(Offsets, []byte(topic), Int32ToBytes(partition)))

		if err != nil {
			return err
		}

		blob, err = item.Value()

		if err != nil {
			return err
		}

		return nil //no error
	})

	if err != nil {
		return 0, err
	}

	return BytesToInt64(blob), nil
}

func (db *DB) FindMessage(topic string, partition int32, offset int64) (*Message, error) {

	key := Key(MessagesByTopicPartitionOffset, []byte(topic), Int32ToBytes(partition), Int64ToBytes(offset))

	var blob []byte

	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)

		if err != nil {
			return err
		}

		blob, err = item.Value()

		if err != nil {
			return err
		}

		return nil //no error
	})

	if err != nil {
		return nil, err
	}

	return MessageFromBytes(blob)
}

func (db *DB) Save(msg *Message) error {

	err := db.db.Update(func(txn *badger.Txn) error {
		key := Key(MessagesByTopicPartitionOffset, []byte(msg.Topic), Int32ToBytes(msg.Partition), Int64ToBytes(msg.Offset))

		blob, err := msg.ToBytes()

		if err != nil {
			return err
		}

		err = txn.Set(key, blob, 0x00)

		if err != nil {
			return err
		}

		//update offsets
		offsetKey := Key(Offsets, []byte(msg.Topic), Int32ToBytes(msg.Partition))

		return txn.Set(
			offsetKey,
			Int64ToBytes(msg.Offset+1), //next offset
			0x00)
	})

	if err != nil {
		Log.Println("There were error during indexing message. Will retry. Err=", err)
	}

	return err
}
