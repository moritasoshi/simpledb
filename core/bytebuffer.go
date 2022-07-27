package core

type ByteBuffer struct {
	hb       []byte
	position int
}

func NewByteBuffer(blockSize int) *ByteBuffer {
	return &ByteBuffer{
		hb:       make([]byte, blockSize),
		position: 0,
	}
}

func (bb *ByteBuffer) Position(newPosition int) *ByteBuffer {
	bb.position = newPosition
	return bb
}

func (bb *ByteBuffer) PutInt(size int) {}
func (bb *ByteBuffer) GetInt()         {}
func (bb *ByteBuffer) Put(src []byte) {

}
