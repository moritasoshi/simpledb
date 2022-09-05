package bytes

import (
	"errors"
	"fmt"
	"io"
)

type Buffer struct {
	buf []byte
	cap int
	off int
}

const INT64_BYTES = 8

var (
	ErrBufferOverflow = errors.New("buffer overflow")
	ErrInvalidBufSize = errors.New("invalid buffer size")
	ErrInvalidOffset  = errors.New("invalid offset")
	ErrOutOfRange     = errors.New("out of range")
)

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

func (bb *Buffer) Write(b []byte) (int, error) {
	if bb.off+len(b) > bb.cap {
		return 0, fmt.Errorf("bytes.Buffer: Write: %w", ErrBufferOverflow)
	}
	cnt := copy(bb.buf[bb.off:], b)
	bb.off += cnt
	return cnt, nil
}

func (bb *Buffer) Read(b []byte) (int, error) {
	cnt := copy(b, bb.buf[bb.off:])
	n := bb.off + cnt
	var err error
	if n == bb.cap {
		err = io.EOF
	} else if n > bb.cap {
		panic("bytes.Buffer: Read: invalid read count")
	}
	bb.off += cnt
	return cnt, err
}

func (bb *Buffer) Seek(offset int) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("bytes.Buffer: Seek: %w", ErrInvalidOffset)
	}
	if offset > bb.cap {
		return 0, fmt.Errorf("bytes.Buffer: Seek: %w", ErrOutOfRange)
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
