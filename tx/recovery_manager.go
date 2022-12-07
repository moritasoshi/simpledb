package tx

import (
	"fmt"

	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
	"github.com/moritasoshi/simpledb/util"
)

// The RecoveryManager logs information about the transaction's activities, in order to be able to roll back a transaction.

type RecoveryManager struct {
	lm    *log.Manager
	bm    *buffer.Manager
	tx    *Transaction
	txNum int
}

func NewRecoveryManager(tx *Transaction, txNum int, lm *log.Manager, bm *buffer.Manager) *RecoveryManager {
	writeLog(lm, txNum, START)
	return &RecoveryManager{
		tx:    tx,
		txNum: txNum,
		lm:    lm,
		bm:    bm,
	}
}

// commitログをディスクに書き込む
func (rm *RecoveryManager) Commit() {
	rm.bm.FlushAll(rm.txNum)
	lsn := writeLog(rm.lm, rm.txNum, COMMIT)
	rm.lm.Flush(lsn)
}

// rollbackログをディスクに書き込む
func (rm *RecoveryManager) Rollback() {
	rm.doRollback()
	rm.bm.FlushAll(rm.txNum)
	lsn := writeLog(rm.lm, rm.txNum, ROLLBACK)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryManager) Recover() {
	rm.doRecover()
	rm.bm.FlushAll(rm.txNum)
	lsn := writeLog(rm.lm, rm.txNum, CHECKPOINT)
	rm.lm.Flush(lsn)
}

// Records the value before change
func (rm *RecoveryManager) SetInt(buf *buffer.Buffer, offset int, val int) int {
	oldVal, err := buf.Contents().GetInt(offset)
	fmt.Printf("RecoveryManager.SetInt(%+v, %v, %v) has oldVal: %v\n", buf, offset, val, oldVal)
	if err != nil {
		panic(err)
	}
	return writeSetIntLog(rm.lm, rm.txNum, buf.Block(), offset, oldVal)
}
func (rm *RecoveryManager) SetString(buf *buffer.Buffer, offset int, val string) int {
	oldVal, err := buf.Contents().GetString(offset)
	if err != nil {
		panic(err)
	}
	return writeSetStringLog(rm.lm, rm.txNum, buf.Block(), offset, oldVal)
}

// Iterates through the log record.
// Each time it calls the record's Undo method until it finds the START record.
func (rm *RecoveryManager) doRollback() {
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		b := iter.Next()
		rec := CreateLogRecorder(b)
		if rec.TxNumber() == rm.txNum {
			if rec.Op() == START {
				return
			}
			rec.Undo(rm.tx)
		}
	}
}

// Rolls back all uncompleted transactions.
func (rm *RecoveryManager) doRecover() {
	var finished map[int]struct{}
	iter := rm.lm.Iterator()
	for iter.HasNext() {
		b := iter.Next()
		rec := CreateLogRecorder(b)
		if rec.Op() == CHECKPOINT {
			return
		}
		if rec.Op() == COMMIT || rec.Op() == ROLLBACK {
			finished[rec.TxNumber()] = struct{}{}
		} else if _, ok := finished[rec.TxNumber()]; ok {
			rec.Undo(rm.tx)
		}
	}

}

// Write to log records
func writeLog(lm *log.Manager, txNum int, recordType int) int {
	switch recordType {
	case START, COMMIT, ROLLBACK:
		rec := make([]byte, 2*util.INT64_BYTES)
		p, err := file.NewPageBytes(rec)
		if err != nil {
			panic(err)
		}
		p.SetInt(0, recordType)
		p.SetInt(util.INT64_BYTES, txNum)
		return lm.Append(p.Contents())
	case CHECKPOINT:
		rec := make([]byte, util.INT64_BYTES)
		p, err := file.NewPageBytes(rec)
		if err != nil {
			panic(err)
		}
		p.SetInt(0, recordType)
		return lm.Append(p.Contents())
	default:
		panic("recordType invalid")
	}
}

// Add SetInt log record which contains the information of record type, txNum, filename, blk number, offsett, value
func writeSetIntLog(lm *log.Manager, txNum int, blk *file.BlockId, offset int, val int) int {
	tpos := util.INT64_BYTES
	fpos := tpos + util.INT64_BYTES
	bpos := fpos + file.MaxLength(len(blk.Filename()))
	opos := bpos + util.INT64_BYTES
	vpos := opos + util.INT64_BYTES
	rec := make([]byte, vpos+util.INT64_BYTES)
	p, err := file.NewPageBytes(rec)
	if err != nil {
		panic(err)
	}

	p.SetInt(0, SETINT)
	p.SetInt(tpos, txNum)
	p.SetString(fpos, blk.Filename())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetInt(vpos, val)

	return lm.Append(p.Contents())
}
func writeSetStringLog(lm *log.Manager, txNum int, blk *file.BlockId, offset int, val string) int {
	tpos := util.INT64_BYTES
	fpos := tpos + util.INT64_BYTES
	bpos := fpos + file.MaxLength(len(blk.Filename()))
	opos := bpos + util.INT64_BYTES
	vpos := opos + util.INT64_BYTES
	rec := make([]byte, vpos+file.MaxLength(len(val)))
	p, err := file.NewPageBytes(rec)
	if err != nil {
		panic(err)
	}

	p.SetInt(0, SETSTRING)
	p.SetInt(tpos, txNum)
	p.SetString(fpos, blk.Filename())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetString(vpos, val)

	return lm.Append(p.Contents())
}
