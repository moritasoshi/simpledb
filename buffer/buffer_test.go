package buffer_test

import (
	"fmt"
	"testing"

	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
)

func TestBuffer(t *testing.T) {
	fm, _ := file.NewManager("buffertest", 400)
	lm := log.NewManager(fm, "logtest")
	bm := buffer.NewManager(fm, lm, 3)

	buf1, _ := bm.Pin(file.NewBlockId("testfile", 1))
	p := buf1.Contents()
	n, _ := p.GetInt(80)
	p.SetInt(80, n+1)
	buf1.SetModified(1, 0)
	fmt.Println("The new value is", n+1)
	bm.Unpin(buf1)

	buf2, _ := bm.Pin(file.NewBlockId("testfile", 2))
	bm.Pin(file.NewBlockId("testfile", 3))
	bm.Pin(file.NewBlockId("testfile", 4))

	bm.Unpin(buf2)
	buf2, _ = bm.Pin(file.NewBlockId("testfile", 1))
	p2 := buf2.Contents()
	p2.SetInt(80, 9999)
	buf2.SetModified(1, 0)
}
