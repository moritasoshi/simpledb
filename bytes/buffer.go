package bytes

import (
	"errors"
	"fmt"
)

var (
	ErrBufferOverflow   = errors.New("buffer overflow error")
	ErrInvalidBufSize   = errors.New("invalid buffer size error, should be numeric number")
	ErrInvalidOffset    = errors.New("invalid offset error, should be numeric number")
	ErrOffsetOutOfRange = errors.New("offset out of range error")
)

type Buffer struct {
	buf []byte
	cap int
	off int
}

const INT64_BYTES = 8

func NewBuffer(bufSize int) (*Buffer, error) {
	if bufSize < 0 {
		return nil, fmt.Errorf("buffer.NewBuffer: %w", ErrInvalidBufSize)
	}
	return &Buffer{
		buf: make([]byte, bufSize),
		cap: bufSize,
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
		return 0, fmt.Errorf("buffer.Write: %w", ErrBufferOverflow)
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
	if offset < 0 {
		return 0, fmt.Errorf("buffer.Seek: %w", ErrInvalidOffset)
	}
	if offset > bb.cap {
		return 0, fmt.Errorf("buffer.Seek: %w", ErrOffsetOutOfRange)
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
