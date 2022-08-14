package core

type ByteBuffer struct {
	buf []byte
	off int
}

func NewByteBuffer(blockSize int) *ByteBuffer {
	return &ByteBuffer{
		buf: make([]byte, blockSize),
		off: 0,
	}
}

func (bb *ByteBuffer) Write(b []byte) (int, error) {
	cnt := copy(bb.buf[bb.off:], b)
	bb.off += cnt
	return cnt, nil
}

func (bb *ByteBuffer) Read(b []byte) (int, error) {
	cnt := copy(b, bb.buf[bb.off:])
	bb.off += cnt
	return cnt, nil
}

func (bb *ByteBuffer) Seek(offset int) (int, error) {
	bb.off = offset
	return offset, nil
}
