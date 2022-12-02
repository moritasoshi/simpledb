package tx_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
	"github.com/moritasoshi/simpledb/tx"
	"github.com/moritasoshi/simpledb/util"
)

func TestRollback(t *testing.T) {
	os.RemoveAll("rollbacktest")
	fm, _ := file.NewManager("rollbacktest", 400)
	lm := log.NewManager(fm, "logtest")
	bm := buffer.NewManager(fm, lm, 2)

	blk0 := file.NewBlockId("testfile", 0)

	// initialize
	tx1 := tx.NewTransaction(fm, lm, bm)
	tx1.Pin(blk0)
	pos := 0
	for i := 0; i < 6; i++ {
		tx1.SetInt(blk0, pos, pos, false)
		pos += util.INT64_BYTES
	}
	tx1.SetString(blk0, pos, "abc", false)
	tx1.Commit()

	// assert
	p0, _ := file.NewPage(fm.BlockSize())
	fm.Read(blk0, p0)
	pos = 0
	for i := 0; i < 6; i++ {
		val0, _ := p0.GetInt(pos)
		if val0 != pos {
			t.Errorf("want %v got %v", pos, val0)
		}
		pos += util.INT64_BYTES
	}
	val0, _ := p0.GetString(pos)
	if val0 != "abc" {
		t.Errorf("want %v got %v", "abc", val0)
	}

	// modify
	tx2 := tx.NewTransaction(fm, lm, bm)
	tx2.Pin(blk0)
	pos = 0
	for i := 0; i < 6; i++ {
		tx2.SetInt(blk0, pos, pos+100, false)
		pos += util.INT64_BYTES
	}
	tx2.SetString(blk0, pos, "xyz", false)
	bm.FlushAll(tx2.Txnum())

	// assert
	p0, _ = file.NewPage(fm.BlockSize())
	fm.Read(blk0, p0)
	pos = 0
	for i := 0; i < 6; i++ {
		val0, _ := p0.GetInt(pos)
		if val0 != pos+100 {
			t.Errorf("want %v got %v", pos+100, val0)
		}
		pos += util.INT64_BYTES
	}
	val0, _ = p0.GetString(pos)
	if val0 != "xyz" {
		t.Errorf("want %v got %v", "xyz", val0)
	}
}

func TestRecovery(t *testing.T) {
	os.RemoveAll("recoverytest")
	fm, _ := file.NewManager("recoverytest", 400)
	lm := log.NewManager(fm, "logtest")
	bm := buffer.NewManager(fm, lm, 8)
	blk0 := file.NewBlockId("testfile", 0)
	blk1 := file.NewBlockId("testfile", 1)

	if fm.CountBlocks("testfile") == 0 {
		// initialize
		tx1 := tx.NewTransaction(fm, lm, bm)
		tx2 := tx.NewTransaction(fm, lm, bm)
		tx1.Pin(blk0)
		tx2.Pin(blk1)
		pos := 0
		for i := 0; i < 6; i++ {
			tx1.SetInt(blk0, pos, pos, false)
			tx2.SetInt(blk1, pos, pos, false)
			pos += util.INT64_BYTES
		}
		tx1.SetString(blk0, pos, "abc", false)
		tx2.SetString(blk1, pos, "def", false)
		tx1.Commit()
		tx2.Commit()

		// assert
		p0, _ := file.NewPage(fm.BlockSize())
		p1, _ := file.NewPage(fm.BlockSize())
		fm.Read(blk0, p0)
		fm.Read(blk1, p1)
		pos = 0
		for i := 0; i < 6; i++ {
			val0, _ := p0.GetInt(pos)
			fmt.Print(val0, " ")
			if val0 != pos {
				t.Errorf("want %v got %v", pos, val0)
			}
			val1, _ := p1.GetInt(pos)
			fmt.Print(val1, " ")
			if val1 != pos {
				t.Errorf("want %v got %v", pos, val1)
			}
			pos += util.INT64_BYTES
		}
		val0, _ := p0.GetString(pos)
		fmt.Print(val0, " ")
		if val0 != "abc" {
			t.Errorf("want %v got %v", "abc", val0)
		}
		val1, _ := p1.GetString(pos)
		fmt.Print(val1, " ")
		if val1 != "def" {
			t.Errorf("want %v got %v", "def", val0)
		}
		fmt.Println()

		// modify
		tx3 := tx.NewTransaction(fm, lm, bm)
		tx4 := tx.NewTransaction(fm, lm, bm)
		tx3.Pin(blk0)
		tx4.Pin(blk1)
		pos = 0

	} else {
		// recover
	}
}
