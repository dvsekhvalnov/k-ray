package db

import (
	"time"

	"github.com/dgraph-io/badger"
	. "github.com/dvsekhvalnov/k-ray/log"
)

type Datastore interface {
	Close()

	SaveMessage(msg *Message) error
	FindMessage(topic string, partition int32, offset int64) (*Message, error)

	SaveOffset(topic string, partition int32, offset int64) error
	FindOffset(topic string, partition int32) (int64, error)

	SearchMessagesByTime(search *SearchRequest) (*SearchResponse, error)
}

func (db *DB) SaveOffset(topic string, partition int32, offset int64) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(
			Key(Offsets, StringToBytes(topic), Int32ToBytes(partition)),
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
		item, err := txn.Get(Key(Offsets, StringToBytes(topic), Int32ToBytes(partition)))

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

	key := Key(MessagesByTopicPartitionOffset, StringToBytes(topic), Int32ToBytes(partition), Int64ToBytes(offset))

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

func (db *DB) SaveMessage(msg *Message) error {

	err := db.Update(saveMsg(msg), saveOffset(msg), indexTimestamp(msg))

	if err != nil {
		Log.Println("There were error during indexing message. Will retry. Err=", err)
	}

	return err
}

func (db *DB) SearchMessagesByTime(search *SearchRequest) (*SearchResponse, error) {
	result := &SearchResponse{
		Rows: make([]*Message, 0),
	}

	start := search.Offset

	if len(start) == 0 {
		start = Key(TimestampIndex, Int64ToBytes(search.Earliest))
	}

	end := Key(TimestampIndex, Int64ToBytes(search.Latest))

	startInstant := time.Now()

	err := db.db.View(func(txn *badger.Txn) error {

		prefix := func(start, end, current []byte, index int) []byte {
			return current[0:len(end)]
		}

		ref := func(start, end, current []byte, index int) []byte {
			return current[len(end):]
		}

		refs := keyRangeScan(txn, search.SkipRows(), start, end, prefix, ref, paginate(search, result))
		result.Total = len(refs)

		fetchAll(txn, refs, search.Paging.Limit,
			func(ref []byte) []byte {
				return Key(MessagesByTopicPartitionOffset, ref)
			},
			func(blob []byte) {
				if msg, err := MessageFromBytes(blob); err == nil {
					result.Rows = append(result.Rows, msg)
				}
			})

		return nil
	})

	result.Took = Millis(int64(time.Since(startInstant)))

	if len(result.Rows) > 0 {
		result.Earliest = Millis(result.Rows[0].Timestamp)
		result.Latest = Millis(result.Rows[len(result.Rows)-1].Timestamp)
	}

	if err != nil {
		return nil, err
	}

	return result, err
}

func indexTimestamp(m *Message) Operation {
	key := Key(TimestampIndex, Int64ToBytes(m.Timestamp), StringToBytes(m.Topic), Int32ToBytes(m.Partition), Int64ToBytes(m.Offset))

	return set(key, nil, 0x00)
}

func saveMsg(m *Message) Operation {
	key := Key(MessagesByTopicPartitionOffset, StringToBytes(m.Topic), Int32ToBytes(m.Partition), Int64ToBytes(m.Offset))
	value, err := m.ToBytes()

	return func(txn *badger.Txn) error {
		if err != nil {
			return err
		}

		return txn.Set(key, value, 0x00)
	}
}

func saveOffset(m *Message) Operation {
	key := Key(Offsets, StringToBytes(m.Topic), Int32ToBytes(m.Partition))
	value := Int64ToBytes(m.Offset + 1) //next offset

	return set(key, value, 0x00)
}

func set(key, value []byte, userMeta byte) Operation {
	return func(txn *badger.Txn) error {
		return txn.Set(key, value, 0x00)
	}
}
