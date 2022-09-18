package buffer_test

import (
	"fmt"
	"testing"

	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
)

func TestManager(t *testing.T) {
	var err error
	fm, _ := file.NewManager("buffertest", 400)
	lm := log.NewManager(fm, "logtest")
	bm := buffer.NewManager(fm, lm, 3)

	buffers := make([]*buffer.Buffer, 6)

	buffers[0], _ = bm.Pin(file.NewBlockId("testfile", 0))
	buffers[1], _ = bm.Pin(file.NewBlockId("testfile", 1))
	buffers[2], _ = bm.Pin(file.NewBlockId("testfile", 2))

	bm.Unpin(buffers[1])
	buffers[1] = nil

	buffers[3], _ = bm.Pin(file.NewBlockId("testfile", 0))
	buffers[4], _ = bm.Pin(file.NewBlockId("testfile", 1))
	fmt.Printf("Available buffers: %v\n", bm.Available())
	fmt.Println("Attempting to pin block 3...")

	buffers[5], err = bm.Pin(file.NewBlockId("testfile", 3))
	if err == buffer.ErrOperationAborted {
		fmt.Println("Error: No available buffers.", err)
	}
	bm.Unpin(buffers[2])
	buffers[2] = nil
	buffers[5], _ = bm.Pin(file.NewBlockId("testfile", 3))

	fmt.Println("Final Buffer Allocation:")
	printBuffers(buffers)
}

func printBuffers(buffers []*buffer.Buffer) {
	for i := range buffers {
		b := buffers[i]
		if b != nil {
			fmt.Printf("buf[%d] pinned to block %v\n", i, b.Block())
		}
	}
}
