package file

import "log"

type BlockId struct {
	filename string
	blknum   int
}

func NewBlockId(filename string, blknum int) *BlockId {
	if blknum < 0 {
		log.Fatal("block number should be natural number")
	}
	if len(filename) <= 0 {
		log.Fatal("filename should be 1 character or more")
	}
	return &BlockId{
		filename: filename,
		blknum:   blknum,
	}
}

func (blk *BlockId) Number() int {
	return blk.blknum
}

func (blk *BlockId) Filename() string {
	return blk.filename
}
