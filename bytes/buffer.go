package bytes

import (
	"errors"
)

var ErrOutOfRange = errors.New("bytes.Buffer: out of range")

type Buffer struct {
	buf []byte
	cap int
	off int
}

const INT64_BYTES = 8

func NewBuffer(blockSize int) (*Buffer, error) {
	if blockSize < 0 {
		return nil, ErrOutOfRange
	}
	return &Buffer{
		buf: make([]byte, blockSize),
		cap: blockSize,
		off: 0,
	}, nil
}

func NewBufferWithBytes(b []byte) *Buffer {
	return &Buffer{
		buf: b,
		cap: len(b),
		off: len(b),
	}
}

func (bb *Buffer) Write(b []byte) (int, error) {
	if bb.off+len(b) > bb.cap {
		return 0, ErrOutOfRange
	}
	cnt := copy(bb.buf[bb.off:], b)
	bb.off += cnt
	return cnt, nil
}

func (bb *Buffer) Read(b []byte) (int, error) {
	cnt := copy(b, bb.buf[bb.off:])
	bb.off += cnt
	return cnt, nil
}

func (bb *Buffer) Seek(offset int) (int, error) {
	if offset < 0 || offset > bb.cap {
		return 0, ErrOutOfRange
	}
	bb.off = offset
	return offset, nil
}

func (bb *Buffer) Cap() int {
	return bb.cap
}

func (bb *Buffer) Buf() []byte {
	return bb.buf
}
