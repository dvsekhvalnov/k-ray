package db

import "encoding/binary"

func Key(table byte, parts ...[]byte) []byte {
	var result []byte = []byte{table}

	for _, arr := range parts {
		result = append(result, arr...)
	}

	return result
}

func BytesToInt64(blob []byte) int64 {
	return int64(binary.BigEndian.Uint64(blob))
}

func UInt64ToBytes(value uint64) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, value)

	return result
}

func Int64ToBytes(value int64) []byte {
	return UInt64ToBytes(uint64(value))
}

func UInt32ToBytes(value uint32) []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, value)

	return result
}

func Int32ToBytes(value int32) []byte {
	return UInt32ToBytes(uint32(value))
}
