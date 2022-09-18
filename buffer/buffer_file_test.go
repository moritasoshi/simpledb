package buffer_test

import (
	"fmt"
	"testing"

	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
)

func TestBufferFile(t *testing.T) {
	fm, _ := file.NewManager("bufferfiletest", 400)
	lm := log.NewManager(fm, "logfile")
	bm := buffer.NewManager(fm, lm, 3)
	blk := file.NewBlockId("testfile", 2)
	pos1 := 88

	b1, _ := bm.Pin(blk)
	p1 := b1.Contents()
	p1.SetString(pos1, "abcdefghijklm")
	size := file.MaxLength(len("abcdefghijklm"))
	pos2 := pos1 + size
	p1.SetInt(pos2, 345)
	b1.SetModified(1, 0)
	bm.Unpin(b1)

	b2, _ := bm.Pin(blk)
	p2 := b2.Contents()

	a, _ := p2.GetInt(pos2)
	b, _ := p2.GetString(pos1)
	fmt.Printf("offset %d contains %v\n", pos2, a)
	fmt.Printf("offset %d contains %v\n", pos1, b)
	bm.Unpin(b2)
}
