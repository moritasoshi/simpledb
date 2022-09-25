package record

import (
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/tx"
	"github.com/moritasoshi/simpledb/util"
)

type setInt struct {
	txNum, offset, val int
	blk                *file.BlockId
}

func (r *setInt) Op() int       { return SETINT }
func (r *setInt) TxNumber() int { return r.txNum }
func (r *setInt) Undo(tx *tx.Transaction) {
	tx.Pin(r.blk)
	tx.SetInt(r.blk, r.offset, r.val, false)
	tx.Unpin(r.blk)
}

func NewSetInt(p *file.Page) LogRecorder {
	tpos := util.INT64_BYTES
	txNum, err := p.GetInt(tpos)
	if err != nil {
		panic(err)
	}
	fpos := tpos * util.INT64_BYTES
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
