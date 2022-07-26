package file

type BlockId struct {
	filename string
	blknum   int
}

func (a *BlockId) Equals(b *BlockId) bool {
	if a == nil || b == nil {
		return false
	}
	return a.filename == b.filename && a.blknum == b.blknum
}

func NewBlockId(filename string, blknum int) *BlockId {
	if len(filename) <= 0 {
		panic("filename should be 1 character or more")
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
