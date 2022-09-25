package tx

import (
	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
	"github.com/moritasoshi/simpledb/util"
)

type Manager struct {
	lm    *log.Manager
	bm    *buffer.Manager
	tx    *Transaction
	txNum int
}

func NewManager(tx *Transaction, txNum int, lm *log.Manager, bm *buffer.Manager) *Manager {
	writeLog(lm, txNum, START)
	return &Manager{
		tx:    tx,
		txNum: txNum,
		lm:    lm,
		bm:    bm,
	}
}

func (rm *Manager) Commit() {
	rm.bm.FlushAll(rm.txNum)
	lsn := writeLog(rm.lm, rm.txNum, COMMIT)
	rm.lm.Flush(lsn)
}

func (rm *Manager) Rollback() {
	rm.doRollback()
	rm.bm.FlushAll(rm.txNum)
	lsn := writeLog(rm.lm, rm.txNum, ROLLBACK)
	rm.lm.Flush(lsn)
}

func (rm *Manager) Recover() {
	rm.doRecover()
	rm.bm.FlushAll(rm.txNum)
	lsn := writeLog(rm.lm, rm.txNum, CHECKPOINT)
	rm.lm.Flush(lsn)
}

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
		return lm.Append(rec)
	case CHECKPOINT:
		rec := make([]byte, util.INT64_BYTES)
		p, err := file.NewPageBytes(rec)
		if err != nil {
			panic(err)
		}
		p.SetInt(0, recordType)
		return lm.Append(rec)
	default:
		return 0
	}
}

func (rm *Manager) doRollback() {
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

func (rm *Manager) doRecover() {
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
