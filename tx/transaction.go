package tx

import (
	"fmt"

	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
)

var nextTxNum = 0

const END_OF_FILE = -1

type Transaction struct {
	txnum     int
	mybuffers *BufferList

	recoveryMgr *RecoveryManager
	concurMgr   *ConcurrentManager

	bm *buffer.Manager
	fm *file.Manager
}

func NewTransaction(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) *Transaction {
	tx := &Transaction{
		fm:    fm,
		bm:    bm,
		txnum: nextTxNumber(),
	}
	tx.recoveryMgr = NewRecoveryManager(tx, tx.txnum, lm, bm)
	tx.concurMgr = NewConcurrentManager()
	tx.mybuffers = NewBufferList(bm)
	return tx
}

func (tx *Transaction) Commit() {
	tx.recoveryMgr.Commit()
	fmt.Println("transaction", tx.txnum, "committed.")
	tx.concurMgr.Release()
	tx.mybuffers.unpinAll()
}

func (tx *Transaction) Rollback() {
	tx.recoveryMgr.Rollback()
	fmt.Println("transaction", tx.txnum, "rollback.")
	tx.concurMgr.Release()
	tx.mybuffers.unpinAll()
}

func (tx *Transaction) Recover() {
	tx.bm.FlushAll(tx.txnum)
	tx.recoveryMgr.Recover()
}

func (tx *Transaction) Pin(blk *file.BlockId)   { tx.mybuffers.pin(blk) }
func (tx *Transaction) Unpin(blk *file.BlockId) { tx.mybuffers.unpin(blk) }

func (tx *Transaction) GetInt(blk *file.BlockId, offset int) int {
	tx.concurMgr.SLock(blk)
	buf := tx.mybuffers.getBuffer(blk)
	i, err := buf.Contents().GetInt(offset)
	if err != nil {
		panic(err)
	}
	return i
}
func (tx *Transaction) GetString(blk *file.BlockId, offset int) string {
	tx.concurMgr.SLock(blk)
	buf := tx.mybuffers.getBuffer(blk)
	val, err := buf.Contents().GetString(offset)
	if err != nil {
		panic(err)
	}
	return val
}
func (tx *Transaction) SetInt(blk *file.BlockId, offset int, val int, okToLog bool) {
	tx.concurMgr.XLock(blk)
	buf := tx.mybuffers.getBuffer(blk)
	lsn := -1
	if okToLog {
		tx.recoveryMgr.SetInt(buf, offset, val)
	}
	p := buf.Contents()
	p.SetInt(offset, val)
	buf.SetModified(tx.txnum, lsn)
}
func (tx *Transaction) SetString(blk *file.BlockId, offset int, val string, okToLog bool) {
	tx.concurMgr.XLock(blk)
	buf := tx.mybuffers.getBuffer(blk)
	lsn := -1
	if okToLog {
		lsn = tx.recoveryMgr.SetString(buf, offset, val)
	}
	p := buf.Contents()
	p.SetString(offset, val)
	buf.SetModified(tx.txnum, lsn)
}

func (tx *Transaction) Size(filename string) int {
	dummyBlk := file.NewBlockId(filename, END_OF_FILE)
	tx.concurMgr.SLock(dummyBlk)
	return tx.fm.CountBlocks(filename)
}

func (tx *Transaction) Append(filename string) *file.BlockId {
	dummyBlk := file.NewBlockId(filename, END_OF_FILE)
	tx.concurMgr.XLock(dummyBlk)
	return tx.fm.Append(filename)
}
func (tx *Transaction) BlockSize() int        { return tx.fm.BlockSize() }
func (tx *Transaction) availableBuffers() int { return tx.bm.Available() }
func (tx *Transaction) Txnum() int            { return tx.txnum }

func nextTxNumber() int {
	nextTxNum++
	return nextTxNum
}
