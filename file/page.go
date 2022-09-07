package file

import (
	"errors"
	"fmt"

	"github.com/moritasoshi/simpledb/bytes"
	"github.com/moritasoshi/simpledb/util"
)

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

const INT64_BYTES = 8

var ErrTooLarge = errors.New("too large")

func (p *Page) GetString(offset int) (string, error) {
	buf, err := p.get(offset)
	if err != nil {
		return "", fmt.Errorf("file.Page: GetString: %w", err)
	}
	return string(buf), nil
}
func (p *Page) SetString(offset int, s string) error {
	b := []byte(s)
	err := p.set(offset, b)
	if err != nil {
		return fmt.Errorf("page.SetString: %w", err)
	}
	return nil
}
func (p *Page) GetInt(offset int) (int, error) {
	buf, err := p.get(offset)
	if err != nil {
		return 0, fmt.Errorf("file.Page: GetInt: %w", err)
	}
	return int(util.BytesToInt64(buf)), nil
}
func (p *Page) SetInt(offset int, i int) error {
	b := util.Int64ToBytes(int64(i))
	if err := p.set(offset, b); err != nil {
		return fmt.Errorf("file.Page: SetInt: %w", err)
	}
	return nil
}
func (p *Page) GetBytes(offset int) ([]byte, error) {
	buf, err := p.get(offset)
	if err != nil {
		return nil, fmt.Errorf("file.Page: GetBytes: %w", err)
	}
	return buf, nil
}
func (p *Page) SetBytes(offset int, b []byte) error {
	if err := p.set(offset, b); err != nil {
		return fmt.Errorf("file.Page: SetBytes: %w", err)
	}
	return nil
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
func (p *Page) set(offset int, b []byte) error {
	total := MaxLength(len(b))
	if total+offset > p.bb.Cap() {
		return fmt.Errorf("page.set: %w", ErrTooLarge)
	}

	// put the size of b
	err := p.setInt64(offset, int64(len(b)))
	if err != nil {
		return fmt.Errorf("page.set: %w", err)
	}

	// put b itself
	err = p.setBytes(offset+INT64_BYTES, b)
	if err != nil {
		return fmt.Errorf("page.set: %w", err)
	}
	return nil
}

func (p *Page) get(offset int) ([]byte, error) {
	len, err := p.getInt64(offset)
	if err != nil {
		return nil, fmt.Errorf("file.Page: getInt64: %w", err)
	}
	b, err := p.getBytes(offset+INT64_BYTES, len)
	if err != nil {
		return nil, fmt.Errorf("file.Page: getInt64: %w", err)
	}
	return b, nil
}
func (p *Page) setInt64(offset int, i int64) error {
	_, err := p.bb.Seek(offset)
	if err != nil {
		return fmt.Errorf("page.setInt64: %w", err)
	}
	_, err = p.bb.Write(util.Int64ToBytes(i))
	if err != nil {
		return fmt.Errorf("page.setInt64: %w", err)
	}
	return nil
}
func (p *Page) getInt64(offset int) (int64, error) {
	buf := make([]byte, INT64_BYTES)
	_, err := p.bb.Seek(offset)
	if err != nil {
		return 0, fmt.Errorf("file.Page: getInt64: %w", err)
	}
	_, err = p.bb.Read(buf)
	if err != nil {
		return 0, fmt.Errorf("file.Page: getInt64: %w", err)
	}
	return util.BytesToInt64(buf), nil
}
func (p *Page) setBytes(offset int, b []byte) error {
	_, err := p.bb.Seek(offset)
	if err != nil {
		return fmt.Errorf("page.setBytes: %w", err)
	}
	_, err = p.bb.Write(b)
	if err != nil {
		return fmt.Errorf("page.setBytes: %w", err)
	}
	return nil
}
func (p *Page) getBytes(offset int, size int64) ([]byte, error) {
	_, err := p.bb.Seek(offset)
	if err != nil {
		return nil, fmt.Errorf("file.Page: getBytes: %w", err)
	}
	buf := make([]byte, size)
	_, err = p.bb.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("file.Page: getBytes: %w", err)
	}
	return buf, nil
}

func NewPage(cap int) (*Page, error) {
	buf, err := bytes.NewBuffer(cap)
	if err != nil {
		return nil, fmt.Errorf("file.Page: NewPage: %w", err)
	}
	return &Page{
		bb: buf,
	}, nil
}

func NewPageBytes(b []byte) (p *Page, err error) {
	l := MaxLength(len(b))
	p, err = NewPage(l)
	if err != nil {
		return nil, fmt.Errorf("file.Page: NewPageWithBytes: %w", err)
	}
	p.SetBytes(0, b)
	return p, nil
}

