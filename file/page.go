package file

import (
	"github.com/moritasoshi/simpledb/bytes"
	"github.com/moritasoshi/simpledb/util"
)

const INT64_BYTES = 8

type Page struct {
	bb *bytes.Buffer
}

type Pager interface {
	GetInt(offset int) int
	GetString(offset int) string
	GetBytes(offset int) []byte
	SetInt(offset int, val int)
	SetString(offset int, val string)
	SetBytes(offset int, val []byte)
}

func NewPage(blockSize int) (*Page, error) {
	buf, err := bytes.NewBuffer(blockSize)
	if err != nil {
		return nil, err
	}
	return &Page{
		bb: buf,
	}, nil
}

func NewPageWithBytes(b []byte) *Page {
	return &Page{
		bb: bytes.NewBufferWithBytes(b),
	}
}

func (p *Page) GetString(offset int) string {
	buf := p.get(offset)
	return string(buf)
}
func (p *Page) SetString(offset int, s string) {
	b := []byte(s)
	p.set(offset, b)
}
func (p *Page) GetInt(offset int) int {
	buf := p.get(offset)
	return int(util.BytesToInt64(buf))
}
func (p *Page) SetInt(offset int, i int) {
	b := util.Int64ToBytes(int64(i))
	p.set(offset, b)
}
func (p *Page) GetBytes(offset int) []byte {
	buf := p.get(offset)
	return buf
}
func (p *Page) SetBytes(offset int, b []byte) {
	p.set(offset, b)
}

func (p *Page) Contents() []byte {
	p.bb.Seek(0)
	buf := make([]byte, p.bb.Cap())
	p.bb.Read(buf)
	return buf
}

func MaxLength(len int) int {
	return INT64_BYTES + len
}

// save a blob as two values: first the number of bytes in the specified blob and then the bytes themselves.
func (p *Page) set(offset int, b []byte) {
	p.setInt64(offset, int64(len(b)))
	p.setBytes(offset+INT64_BYTES, b)
}
func (p *Page) get(offset int) []byte {
	bufSize := p.getInt64(offset)
	return p.getBytes(offset+INT64_BYTES, bufSize)
}
func (p *Page) setInt64(offset int, i int64) {
	p.bb.Seek(offset)
	p.bb.Write(util.Int64ToBytes(i))
}
func (p *Page) getInt64(offset int) int64 {
	buf := make([]byte, INT64_BYTES)
	p.bb.Seek(offset)
	p.bb.Read(buf)
	return util.BytesToInt64(buf)
}
func (p *Page) setBytes(offset int, b []byte) {
	p.bb.Seek(offset)
	p.bb.Write(b)
}
func (p *Page) getBytes(offset int, size int64) []byte {
	p.bb.Seek(offset)
	buf := make([]byte, size)
	p.bb.Read(buf)
	return buf
}
