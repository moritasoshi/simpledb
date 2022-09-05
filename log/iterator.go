package log

import (
	"log"

	"github.com/moritasoshi/simpledb/file"
)

type Iterator struct {
	fm         *file.Manager
	blk        *file.BlockId
	p          *file.Page
	currentPos int
	boundary   int
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

// Determines if the current log record is the earliest record in the log file.
func (iter *Iterator) hasNext() bool {
	return iter.currentPos < iter.fm.BlockSize() || iter.blk.Number() > 0
}

// Moves to the next log record in the block.
func (iter *Iterator) next() []byte {
	if iter.currentPos == iter.fm.BlockSize() {
		iter.blk = file.NewBlockId(iter.blk.Filename(), iter.blk.Number()-1)
		iter.moveToBlock(iter.blk)
	}
	rec := iter.p.GetBytes(iter.currentPos)
	iter.currentPos += INT64_BYTES + len(rec)
	return rec
}

// Moves to the specified block
func (iter *Iterator) moveToBlock(blk *file.BlockId) {
	iter.fm.Read(blk, iter.p)
	iter.boundary = iter.p.GetInt(0)
	iter.currentPos = iter.boundary
}
