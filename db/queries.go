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
		Rows:       make([]*Message, 0),
		Earliest:   Millis(search.Earliest),
		Latest:     Millis(search.Latest),
		Page:       search.Page,
		OffsetUsed: true,
	}

	start := search.Offset
	skip := 0

	//TODO: start, skip, result.OffsetUsed = from(search, TimestampIndex)
	//no offset given, scan from beginning, skip rows according to paging requested
	if len(start) == 0 {
		result.OffsetUsed = false
		start = Key(TimestampIndex, Int64ToBytes(search.Latest)) //FROM (Swap if scanning forward)
		skip = search.SkipRows()
	}

	end := Key(TimestampIndex, Int64ToBytes(search.Earliest)) //TO (Swap if scanning forward)

	startInstant := time.Now()

	err := db.db.View(func(txn *badger.Txn) error {

		prefix := func(start, end, current []byte, index int) []byte {
			return current[0:len(end)]
		}

		ref := func(start, end, current []byte, index int) []byte {
			return current[len(end):]
		}

		Log.Println("[DEBUG] Skipping rows:", skip)

		refs := keyRangeScan(txn, true, skip, start, end, prefix, ref, paginate(search, result))
		result.Total = len(refs)

		Log.Println("[DEBUG] Found refs:", result.Total)

		//adjust total by adding skipped rows if we found something
		//otherwise wrong page probably was requested, return zero as total
		if result.Total > 0 {
			result.Total = result.Total + search.SkipRows()
		} else {
			result.CleanOffsets()
		}

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
