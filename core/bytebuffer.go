package core

type ByteBuffer struct {
	hb       []byte // heap buffers
	position int
}

func (bb *ByteBuffer) Position(newPosition int) (*ByteBuffer) {
	bb.position = newPosition
	return bb
}
