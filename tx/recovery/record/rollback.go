package record

import (
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/tx"
	"github.com/moritasoshi/simpledb/util"
)

type rollback struct {
	txNum int
}

func (r *rollback) Op() int                 { return ROLLBACK }
func (r *rollback) TxNumber() int           { return r.txNum }
func (r *rollback) Undo(tx *tx.Transaction) {}

func NewRollback(p *file.Page) LogRecorder {
	tpos := util.INT64_BYTES
	n, err := p.GetInt(tpos)
	if err != nil {
		panic(err)
	}
	return &rollback{
		txNum: n,
	}
}
