package bytes

import (
	"errors"
	"io"
)

type Buffer struct {
	buf []byte
	cap int
	off int
}

var (
	ErrBufferOverflow = errors.New("bytes.Buffer: buffer overflow")
	ErrOutOfRange     = errors.New("bytes.Buffer: out of range")
)

// String returns the contents of the buffer as a string.
func (b *Buffer) String() string {
	if b == nil {
		return "<nil>"
	}
	return string(b.buf[b.off:])
}

// empty reports whether the unread portion of the buffer is empty.
func (b *Buffer) empty() bool { return len(b.buf) <= b.off }

func (bb *Buffer) Write(b []byte) (int, error) {
	if bb.off+len(b) > bb.cap {
		return 0, ErrBufferOverflow
	}
	cnt := copy(bb.buf[bb.off:], b)
	bb.off += cnt
	return cnt, nil
}

func (bb *Buffer) Read(p []byte) (n int, err error) {
	if bb.empty() {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n = copy(p, bb.buf[bb.off:])
	bb.off += n
	return n, nil
}

func (bb *Buffer) Seek(offset int) (int, error) {
	if offset < 0 || offset > bb.cap {
		return 0, ErrOutOfRange
	}
	bb.off = offset
	return offset, nil
}

func (bb *Buffer) Cap() int { return bb.cap }

func NewBuffer(cap int) (*Buffer, error) {
	if cap < 0 {
		return nil, ErrOutOfRange
	}
	return &Buffer{
		buf: make([]byte, cap),
		cap: cap,
		off: 0,
	}, nil
}
