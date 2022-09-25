package record

import (
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/tx"
)

type LogRecorder interface {
	Op() int
	TxNumber() int
	Undo(tx *tx.Transaction)
}

const (
	CHECKPOINT = 1 << iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

func CreateLogRecorder(b []byte) LogRecorder {
	p, err := file.NewPageBytes(b)
	if err != nil {
		panic(err)
	}
	t, err := p.GetInt(0)
	if err != nil {
		panic(err)
	}
	switch t {
	case CHECKPOINT:
		return NewCheckpoint()
	case START:
		return NewStart(p)
	case COMMIT:
		return NewCommit(p)
	case ROLLBACK:
		return NewRollback(p)
	case SETINT:
		return NewSetInt(p)
	case SETSTRING:
		return NewSetString(p)
	default:
		return nil
	}
}
