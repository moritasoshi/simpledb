package file

type BlockId struct {
	filename string
	blknum   int
}

func NewBlockId(filename string, blknum int) *BlockId {
	return &BlockId{
		filename: filename,
		blknum:   blknum,
	}
}
