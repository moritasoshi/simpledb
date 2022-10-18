package buffer_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
)

func _TestManager(t *testing.T) {
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

// バッファプールへの追加と削除を非同期で実行できること
func TestPinFailure(t *testing.T) {
	var err error
	fm, _ := file.NewManager("buffertest", 400)
	lm := log.NewManager(fm, "logtest")
	bm := buffer.NewManager(fm, lm, 3)

	bm.Pin(file.NewBlockId("testfile", 0))
	bm.Pin(file.NewBlockId("testfile", 1))
	bm.Pin(file.NewBlockId("testfile", 2))

	// バッファプールに空きがない場合はエラーが返ることを確認
	_, err = bm.Pin(file.NewBlockId("testfile", 3))
	if err != buffer.ErrOperationAborted {
		t.Errorf("want %v, got %v", buffer.ErrOperationAborted, err)
	}
}

func TestPinSuccess(t *testing.T) {
	fm, _ := file.NewManager("buffertest", 400)
	lm := log.NewManager(fm, "logtest")
	bm := buffer.NewManager(fm, lm, 2)

	buffers := make([]*buffer.Buffer, 3)

	buffers[0], _ = bm.Pin(file.NewBlockId("testfile", 0))
	buffers[1], _ = bm.Pin(file.NewBlockId("testfile", 1))

	// バッファプールが利用可能になるとピン留めに成功すること
	type result struct {
		buf *buffer.Buffer
		err error
	}
	ch := make(chan result)

	go func() {
		defer close(ch)

		fmt.Println("Pinning...")
		buf, e := bm.Pin(file.NewBlockId("testfile", 2))
		fmt.Println("Pinning finished")
		ch <- result{buf: buf, err: e}
	}()
	time.Sleep(1 * time.Second)

	fmt.Println("Unpinning...")
	bm.Unpin(buffers[1])
	fmt.Println("Unpinning finished")

	res := <-ch
	buffers[2] = res.buf


	// ピン留めに成功していることを確認
	if res.err != nil {
		t.Errorf("want %v, got %v", nil, res.err)
	}
}
