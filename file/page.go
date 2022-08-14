package file

import (
	"github.com/moritasoshi/simpledb/bytes"
	"github.com/moritasoshi/simpledb/util"
)

const INT64_BYTES = 8

type Page struct {
	bb bytes.Buffer
}

type Pager interface {
	GetInt(offset int) int
	GetBytes(offset int) []byte
	GetString(offset int) string
	SetInt(offset int, val int)
	SetBytes(offset int, val []byte)
	SetString(offset int, val string)
}

func NewPage(blockSize int) *Page {
	return &Page{
		bb: *bytes.NewBuffer(blockSize),
	}
}

func (p *Page) SetString(offset int, s string) {
	p.SetInt64(offset, int64(len(s)))
	p.setBytes(offset+INT64_BYTES, []byte(s))
}
func (p *Page) GetString(offset int) string {
	bufSize := p.GetInt64(offset)
	buf := make([]byte, bufSize)
	p.bb.Read(buf)
	return string(buf)
}

func (p *Page) SetInt64(offset int, i int64) {
	p.bb.Seek(offset)
	p.bb.Write(util.Int64ToBytes(i))
}
func (p *Page) GetInt64(offset int) int64 {
	buf := make([]byte, INT64_BYTES)
	p.bb.Seek(offset)
	p.bb.Read(buf)
	return util.BytesToInt64(buf)
}

func (p *Page) setBytes(offset int, b []byte) {
	p.bb.Seek(offset)
	p.bb.Write(b)
}

func (p *Page) MaxLength(len int) int {
	return INT64_BYTES + len
}
