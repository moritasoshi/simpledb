package util

import "encoding/binary"

const INT64_BYTES = 8

func Int64ToBytes(i int64) []byte {
	b := make([]byte, INT64_BYTES)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func BytesToInt64(b []byte) int64 {
	data := binary.BigEndian.Uint64(b)
	return int64(data)
}
