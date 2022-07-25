package file

import (
	"github.com/moritasoshi/simpledb/core"
)

type (
	Page struct {
		bb ByteBuffer
	}
)

const (
	blocksize = 400
)

func NewPage(blockSize int) *Page {
	return &Page{
		bb: make([]byte, blockSize),
	}
}

func setString(offset int, s string) {
	b := []byte(s)
	setBytes(offset, b)
}

func setBytes(offset int, b []byte) {
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
