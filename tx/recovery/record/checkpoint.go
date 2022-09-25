package record

import (
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
	"github.com/moritasoshi/simpledb/tx"
	"github.com/moritasoshi/simpledb/util"
)

type checkpoint struct{}

func (r *checkpoint) Op() int                 { return CHECKPOINT }
func (r *checkpoint) TxNumber() int           { return -1 } // dummy value
func (r *checkpoint) Undo(tx *tx.Transaction) {}

func NewCheckpoint() LogRecorder { return &checkpoint{} }

func WriteToLog(lm *log.Manager) int {
	rec := make([]byte, 2*util.INT64_BYTES)
	p, err := file.NewPageBytes(rec)
	if err != nil {
		return 0
	}
	p.SetInt(0, CHECKPOINT)
	return lm.Append(rec)
}
