package tx

import (
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/util"
)

type LogRecorder interface {
	Op() int
	TxNumber() int
	Undo(tx *Transaction)
}

// There are six kinds of log record.
// A START record is written              when a transaction begins.
// A COMMIT or ROLLBACK record is written when a transaction completes.
// A SETXXX record is written             when a transaction modifies a value.
const (
	CHECKPOINT = iota + 1
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

// checkpoint
type checkpoint struct{}

func NewCheckpoint() LogRecorder           { return &checkpoint{} }
func (r *checkpoint) Op() int              { return CHECKPOINT }
func (r *checkpoint) TxNumber() int        { return -1 } // dummy value
func (r *checkpoint) Undo(tx *Transaction) {}

// commit
type commit struct {
	txNum int
}

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
func (r *commit) Op() int              { return COMMIT }
func (r *commit) TxNumber() int        { return r.txNum }
func (r *commit) Undo(tx *Transaction) {}

// rollback
type rollback struct {
	txNum int
}

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
func (r *rollback) Op() int              { return ROLLBACK }
func (r *rollback) TxNumber() int        { return r.txNum }
func (r *rollback) Undo(tx *Transaction) {}

// start
type start struct {
	txNum int
}

func NewStart(p *file.Page) LogRecorder {
	tpos := util.INT64_BYTES
	txNum, err := p.GetInt(tpos)
	if err != nil {
		panic(err)
	}
	return &start{txNum: txNum}
}
func (r *start) Op() int              { return START }
func (r *start) TxNumber() int        { return r.txNum }
func (r *start) Undo(tx *Transaction) {}

// set int
type setInt struct {
	txNum, offset, val int
	blk                *file.BlockId
}

func NewSetInt(p *file.Page) LogRecorder {
	tpos := util.INT64_BYTES
	txNum, err := p.GetInt(tpos)
	if err != nil {
		panic(err)
	}
	fpos := tpos + util.INT64_BYTES
	filename, err := p.GetString(fpos)
	if err != nil {
		panic(err)
	}
	bpos := fpos + file.MaxLength(len(filename))
	blknum, err := p.GetInt(bpos)
	if err != nil {
		panic(err)
	}
	blk := file.NewBlockId(filename, blknum)
	opos := bpos + util.INT64_BYTES
	offset, err := p.GetInt(opos)
	if err != nil {
		panic(err)
	}
	vpos := opos + util.INT64_BYTES
	val, err := p.GetInt(vpos)
	if err != nil {
		panic(err)
	}

	return &setInt{
		txNum:  txNum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}
func (r *setInt) Op() int       { return SETINT }
func (r *setInt) TxNumber() int { return r.txNum }
func (r *setInt) Undo(tx *Transaction) {
	tx.Pin(r.blk)
	tx.SetInt(r.blk, r.offset, r.val, false)
	tx.Unpin(r.blk)
}

// set string
type setString struct {
	txNum, offset int
	val           string
	blk           *file.BlockId
}

func NewSetString(p *file.Page) LogRecorder {
	tpos := util.INT64_BYTES
	txNum, err := p.GetInt(tpos)
	if err != nil {
		panic(err)
	}
	fpos := tpos + util.INT64_BYTES
	filename, err := p.GetString(fpos)
	if err != nil {
		panic(err)
	}
	bpos := fpos + file.MaxLength(len(filename))
	blknum, err := p.GetInt(bpos)
	if err != nil {
		panic(err)
	}
	blk := file.NewBlockId(filename, blknum)
	opos := bpos + util.INT64_BYTES
	offset, err := p.GetInt(opos)
	if err != nil {
		panic(err)
	}
	vpos := opos + util.INT64_BYTES
	val, err := p.GetString(vpos)
	if err != nil {
		panic(err)
	}
	return &setString{
		txNum:  txNum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}
func (r *setString) Op() int       { return SETSTRING }
func (r *setString) TxNumber() int { return r.txNum }
func (r *setString) Undo(tx *Transaction) {
	tx.Pin(r.blk)
	tx.SetString(r.blk, r.offset, r.val, false)
	tx.Unpin(r.blk)

}
