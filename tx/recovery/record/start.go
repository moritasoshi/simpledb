package record

import (
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/tx"
	"github.com/moritasoshi/simpledb/util"
)

type start struct {
	txNum int
}

func (r *start) Op() int                 { return START }
func (r *start) TxNumber() int           { return r.txNum }
func (r *start) Undo(tx *tx.Transaction) {}

func NewStart(p *file.Page) LogRecorder {
	tpos := 2 * util.INT64_BYTES
	txNum, err := p.GetInt(tpos)
	if err != nil {
		panic(err)
	}
	return &start{txNum: txNum}
}
