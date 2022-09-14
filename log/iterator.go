package log

import (
	"log"

	"github.com/moritasoshi/simpledb/file"
)

type Iterator struct {
	fm       *file.Manager
	blk      *file.BlockId
	p        *file.Page
	pos      int
	boundary int
}

func NewIterator(fm *file.Manager, blk *file.BlockId) *Iterator {
	p, err := file.NewPage(fm.BlockSize())
	if err != nil {
		log.Fatal(err)
	}
	iter := &Iterator{
		fm:  fm,
		blk: blk,
		p:   p,
		// boundary:   boundary,
		// currentpos: boundary,
	}
	iter.moveToBlock(blk)
	return iter
}

// Moves to the specified block
func (iter *Iterator) moveToBlock(blk *file.BlockId) {
	iter.fm.Read(blk, iter.p)
	iter.boundary, _ = iter.p.GetInt(0)
	iter.pos = iter.boundary
}

// Determines if the current log record is the earliest record in the log file.
func (iter *Iterator) hasNext() bool {
	return iter.pos < iter.fm.BlockSize() || iter.blk.Number() > 0
}

// Moves to the next log record in the block.
func (iter *Iterator) next() []byte {
	if iter.pos == iter.fm.BlockSize() {
		iter.blk = file.NewBlockId(iter.blk.Filename(), iter.blk.Number()-1)
		iter.moveToBlock(iter.blk)
	}
	rec, _ := iter.p.GetBytes(iter.pos)
	iter.pos += file.MaxLength(len(rec))
	return rec
}
