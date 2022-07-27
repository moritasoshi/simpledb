package file

import (
	"github.com/moritasoshi/simpledb/core"
)

type (
	Page struct {
		bb core.ByteBuffer
	}
)

const (
	blocksize = 400
)

func NewPage(blockSize int) *Page {
	return &Page{
		bb: *core.NewByteBuffer(blockSize),
	}
}

// 指定した位置offsetに文字列sを配置する
func (p *Page) SetString(offset int, s string) {
	b := []byte(s)
	p.setBytes(offset, b)
}

// 指定した位置offsetにバイト配列bを配置する
func (p *Page) setBytes(offset int, b []byte) {
	p.bb.Position(offset)
	p.bb.PutInt(len(b))
	p.bb.Put(b)
}

func (page *Page) GetUInt32(offset int64) (uint32, error) {
	if page == nil {
		return 0, nil
	}

	// if _, err := page.bb.Seek(offset, io.SeekStart); err != nil {
	// 	return 0, err
	// }

	var ret uint32
	// if err := binary.Read(page.bb, binary.BigEndian, &ret); err != nil {
	// 	return 0, err
	// }

	return ret, nil
}
