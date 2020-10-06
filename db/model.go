package db

import (
	"strconv"

	"github.com/Shopify/Sarama"
	"github.com/vmihailenco/msgpack"
)

type Paging struct {
	Limit int   `json:"limit"`
	Pages []int `json:"pages"`
}

type SearchRequest struct {
	Search   string  `json:"search"`
	Earliest int64   `json:"earliest"`
	Latest   int64   `json:"latest"`
	Page     int     `json:"page"`
	Offset   []byte  `json:"offset"`
	Paging   *Paging `json:"paging"`
}

type SearchResponse struct {
	Total      int               `json:"total"`
	Earliest   int64             `json:"earliest"`
	Latest     int64             `json:"latest"`
	Rows       []*Message        `json:"rows"`
	Page       int               `json:"page"`
	OffsetUsed bool              `json:"offsetUsed"`
	Offsets    map[string][]byte `json:"offsets"`
	Took       int64             `json:"took"`
	TimedOut   bool              `json:"timedOut"`
}

func (r *SearchResponse) TotalPages() int {
	if r.Offsets != nil {
		return len(r.Offsets)
	}

	return 0
}

func (r *SearchResponse) CleanOffsets() {
	r.Offsets = make(map[string][]byte)
}

func (r *SearchResponse) AddOffset(page int, offset []byte) {
	if r.Offsets == nil {
		r.Offsets = make(map[string][]byte)
	}

	r.Offsets[strconv.Itoa(page)] = offset
}

func (r *SearchRequest) SkipRows() int {
	return (r.Page - 1) * r.Paging.Limit
}

type Tag struct {
	Value   string `json:"value"`
	Details string `json:"details,omitempty"`
}

type BinaryData struct {
	Value []byte `json:"value"`
	Size  int    `json:"size"`
	Type  string `json:"type"`
}

type Message struct {
	Key            BinaryData     `json:"key"`
	Value          BinaryData     `json:"value"`
	Topic          string         `json:"topic"`
	Partition      int32          `json:"partition"`
	Offset         int64          `json:"offset"`
	Timestamp      int64          `json:"timestamp"`
	BlockTimestamp int64          `json:"blockTimestamp"`
	Tags           map[string]Tag `json:"tags"`
}

func (m *Message) AddTag(tag, value, details string) {
	m.Tags[tag] = Tag{Value: value, Details: details}
}

func NewMessage(msg *sarama.ConsumerMessage) *Message {
	return &Message{
		Key:            BinaryData{Value: msg.Key, Size: len(msg.Key)},
		Value:          BinaryData{Value: msg.Value, Size: len(msg.Value)},
		Topic:          msg.Topic,
		Partition:      msg.Partition,
		Offset:         msg.Offset,
		Timestamp:      msg.Timestamp.UnixNano(),
		BlockTimestamp: msg.BlockTimestamp.UnixNano(),
		Tags:           make(map[string]Tag),
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
