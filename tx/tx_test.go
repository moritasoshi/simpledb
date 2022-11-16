package tx_test

import (
	"fmt"
	"testing"

	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
	"github.com/moritasoshi/simpledb/tx"
)

func TestTx(t *testing.T) {
	fm, _ := file.NewManager("txtest", 400)
	lm := log.NewManager(fm, "logtest")
	bm := buffer.NewManager(fm, lm, 3)

	tx1 := tx.NewTransaction(fm, lm, bm)
	blk := file.NewBlockId("testfile", 1)
	tx1.Pin(blk)

	tx1.SetInt(blk, 80, 1, false)
	tx1.SetString(blk, 40, "one", false)
	tx1.Commit()

	tx2 := tx.NewTransaction(fm, lm, bm)
	tx2.Pin(blk)
	ival := tx2.GetInt(blk, 80)
	sval := tx2.GetString(blk, 40)
	fmt.Println("initial value at location 80 =", ival)
	fmt.Println("initial value at location 40 =", sval)
	newival := ival + 1
	newsval := sval + "!"

	tx2.SetInt(blk, 80, newival, true)
	tx2.SetString(blk, 40, newsval, true)
	tx2.Commit()

	tx3 := tx.NewTransaction(fm, lm, bm)
	tx3.Pin(blk)
	fmt.Println("new value at location 80 =", tx3.GetInt(blk, 80))
	fmt.Println("new value at location 40 =", tx3.GetString(blk, 40))
	tx3.SetInt(blk, 80, 9999, true)
	fmt.Println("pre-rollback value at location 80 =", tx3.GetInt(blk, 80))
	tx3.Rollback()

	tx4 := tx.NewTransaction(fm, lm, bm)
	tx4.Pin(blk)
	fmt.Println("post-rollback value at location 80 =", tx4.GetInt(blk, 80))
	tx4.Commit()
}
