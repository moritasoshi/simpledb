package bytes

type Buffer struct {
	buf []byte
	cap int
	off int
}

func NewBuffer(blockSize int) *Buffer {
	return &Buffer{
		buf: make([]byte, blockSize),
		cap: blockSize,
		off: 0,
	}
}

func (bb *Buffer) Write(b []byte) (int, error) {
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
	bb.off = offset
	return offset, nil
}

func (bb *Buffer) Cap() int {
	return bb.cap
}
