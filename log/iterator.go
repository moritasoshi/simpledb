package log

// The records returned by the iterator method are in reverse order of addition,
// starting with the most recent record and moving backward through the log file.
// The records are returned in this order because the recovery manager wants to see them.

//
// If the contents of the block are as follows,
//
// -----------------------------------------------------------------------------------
// | block size (400 bytes)                                                          |
// -----------------------------------------------------------------------------------
// |                    |                           ||         ||         ||         |
// | boundary (8 bytes) | ......................... || record3 || record2 || record1 |
// |                    |                           ||         ||         ||         |
// -----------------------------------------------------------------------------------

// for iter.HasNext() {
//     fmt.Println(iter.Next())
// }

// the output will be as follows.
//
// > record3
// > record2
// > record1

import (
	"log"

	"github.com/moritasoshi/simpledb/file"
)

type iterator struct {
	fm       *file.Manager
	blk      *file.BlockId
	p        *file.Page
	pos      int
	boundary int
}

func newIterator(fm *file.Manager, blk *file.BlockId) *iterator {
	p, err := file.NewPage(fm.BlockSize())
	if err != nil {
		log.Fatal(err)
	}
	iter := &iterator{
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
func (iter *iterator) moveToBlock(blk *file.BlockId) {
	iter.fm.Read(blk, iter.p)
	iter.boundary, _ = iter.p.GetInt(0)
	iter.pos = iter.boundary
}

// Determines if the current log record is the earliest record in the log file.
// hasNext returns false when there are no more records in the page and no more previous blocks.
func (iter *iterator) HasNext() bool {
	return iter.pos < iter.fm.BlockSize() || iter.blk.Number() > 0
}

// Next returns the next log record.
// The records are in reverse order of addition.
func (iter *iterator) Next() []byte {
	if iter.pos == iter.fm.BlockSize() {
		iter.blk = file.NewBlockId(iter.blk.Filename(), iter.blk.Number()-1)
		iter.moveToBlock(iter.blk)
	}
	rec, _ := iter.p.GetBytes(iter.pos)
	iter.pos += file.MaxLength(len(rec))
	return rec
}
