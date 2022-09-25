package record

import (
	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/tx"
	"github.com/moritasoshi/simpledb/util"
)

type setString struct {
	txNum, offset int
	val           string
	blk           *file.BlockId
}

func (r *setString) Op() int       { return SETSTRING }
func (r *setString) TxNumber() int { return r.txNum }
func (r *setString) Undo(tx *tx.Transaction) {
	tx.Pin(r.blk)
	tx.SetString(r.blk, r.offset, r.val, false)
	tx.Unpin(r.blk)

}

func NewSetString(p *file.Page) LogRecorder {
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
