package tx

import (
	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
)

type BufferList struct {
	buffers map[*file.BlockId]*buffer.Buffer
	pins    []*file.BlockId
	bm      *buffer.Manager
}

func NewBufferList(bm *buffer.Manager) *BufferList {
	return &BufferList{
		bm: bm,
	}
}

func (blist *BufferList) getBuffer(blk *file.BlockId) *buffer.Buffer {
	b, _ := blist.buffers[blk]
	return b
}

func (blist *BufferList) pin(blk *file.BlockId) error {
	buf, err := blist.bm.Pin(blk)
	if err != nil {
		return err
	}
	blist.buffers[blk] = buf
	blist.pins = append(blist.pins, blk)
	return nil
}

func (blist *BufferList) unpin(blk *file.BlockId) {
	buf := blist.buffers[blk]
	blist.bm.Unpin(buf)

	// remove the specified block from pins.
	var result []*file.BlockId
	for _, b := range blist.pins {
		if !b.Equals(blk) {
			result = append(result, b)
		}
	}
	blist.pins = result
}

func (blist *BufferList) unpinAll() {
	for _, blk := range blist.pins {
		buf := blist.buffers[blk]
		if buf != nil {
			blist.bm.Unpin(buf)
		}
	}
	for k := range blist.buffers {
		delete(blist.buffers, k)
	}
	blist.pins = nil
}
