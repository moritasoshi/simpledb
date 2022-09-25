package record

import (
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/tx"
	"github.com/moritasoshi/simpledb/util"
)

type commit struct {
	txNum int
}

func (r *commit) Op() int                 { return COMMIT }
func (r *commit) TxNumber() int           { return r.txNum }
func (r *commit) Undo(tx *tx.Transaction) {}

func NewCommit(p *file.Page) LogRecorder {
	tpos := util.INT64_BYTES
	n, err := p.GetInt(tpos)
	if err != nil {
		panic(err)
	}
	return &commit{
		txNum: n,
	}
}
