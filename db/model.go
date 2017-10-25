package db

import (
	"github.com/Shopify/Sarama"
	"github.com/vmihailenco/msgpack"
)

type Message struct {
	Key            []byte `json:"key"`
	Value          []byte `json:"value"`
	Topic          string `json:"topic"`
	Partition      int32  `json:"partition"`
	Offset         int64  `json:"offset"`
	Timestamp      int64  `json:"timestamp"`
	BlockTimestamp int64  `json:"blockTimestamp"`
	Size           int    `json:"size"`
}

func NewMessage(msg *sarama.ConsumerMessage) *Message {
	return &Message{
		Key:            msg.Key,
		Value:          msg.Value,
		Topic:          msg.Topic,
		Partition:      msg.Partition,
		Offset:         msg.Offset,
		Timestamp:      msg.Timestamp.UnixNano(),
		BlockTimestamp: msg.BlockTimestamp.UnixNano(),
		Size:           len(msg.Value),
	}
}

func MessageFromBytes(blob []byte) (*Message, error) {
	var out Message
	err := msgpack.Unmarshal(blob, &out)

	return &out, err
}

func (m *Message) ToBytes() ([]byte, error) {
	return msgpack.Marshal(&m)
}
