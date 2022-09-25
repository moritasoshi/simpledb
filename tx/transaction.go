package tx

import (
	"github.com/moritasoshi/simpledb/buffer"
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
)

var nextTxNum = 0

const END_OF_FILE = -1

type Transaction struct {
	txnum     int
	mybuffers *BufferList

	bm *buffer.Manager
	fm *file.Manager
}

func NewTransaction(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) *Transaction {
	return &Transaction{
		fm:    fm,
		bm:    bm,
		txnum: nextTxNumber(),
	}
}

func (tx *Transaction) Pin(blk *file.BlockId) {

}

func nextTxNumber() int {
	nextTxNum++
	return nextTxNum
}
