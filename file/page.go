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

var ErrTooLarge = errors.New("too large")

// SetString set two valuse: length of string and contents themselves.
// That is why SetString needs 8bytes and contents length.
// -----------------------------------------
// | length (8 bytes) | contents (n bytes) |
// -----------------------------------------
func (p *Page) SetString(offset int, s string) error {
	b := []byte(s)
	err := p.setBlob(offset, b)
	if err != nil {
		return fmt.Errorf("page.SetString: %w", err)
	}
	return nil
}
func (p *Page) GetString(offset int) (string, error) {
	buf, err := p.getBlob(offset)
	if err != nil {
		return "", fmt.Errorf("file.Page: GetString: %w", err)
	}
	return string(buf), nil
}

// SetInt set int value that size is 8 bytes.
// -------------------
// | value (8 bytes) |
// -------------------
func (p *Page) SetInt(offset int, i int) error {
	err := p.setInt64(offset, int64(i))
	if err != nil {
		return fmt.Errorf("file.Page: SetInt: %w", err)
	}
	return nil
}
func (p *Page) GetInt(offset int) (int, error) {
	i, err := p.getInt64(offset)
	if err != nil {
		return 0, fmt.Errorf("file.Page: GetInt: %w", err)
	}
	return int(i), nil
}

// SetBytes set two valuse: length of bytes and contents themselves.
// That is why SetBytes needs 8bytes and contents length.
// -----------------------------------------
// | length (8 bytes) | contents (n bytes) |
// -----------------------------------------
func (p *Page) SetBytes(offset int, b []byte) error {
	if err := p.setBlob(offset, b); err != nil {
		return fmt.Errorf("file.Page: SetBytes: %w", err)
	}
	return nil
}
func (p *Page) GetBytes(offset int) ([]byte, error) {
	buf, err := p.getBlob(offset)
	if err != nil {
		return nil, fmt.Errorf("file.Page: GetBytes: %w", err)
	}
	return buf, nil
}

func (p *Page) Contents() []byte {
	p.bb.Seek(0)
	buf := make([]byte, p.bb.Cap())
	p.bb.Read(buf)
	return buf
}
func MaxLength(len int) int { return util.INT64_BYTES + len }

// save a blob as two values: first the number of bytes in the specified blob and then the bytes themselves.
func (p *Page) setBlob(offset int, b []byte) error {
	total := MaxLength(len(b))
	if total+offset > p.bb.Cap() {
		return fmt.Errorf("page.setBlob: %w", ErrTooLarge)
	}

	// put the size of b
	err := p.setInt64(offset, int64(len(b)))
	if err != nil {
		return fmt.Errorf("page.setBlob: %w", err)
	}

	// put b itself
	err = p.setBytes(offset+util.INT64_BYTES, b)
	if err != nil {
		return fmt.Errorf("page.setBlob: %w", err)
	}
	return nil
}
func (p *Page) getBlob(offset int) ([]byte, error) {
	// get the size of contents
	len, err := p.getInt64(offset)
	if err != nil {
		return nil, fmt.Errorf("file.Page: getInt64: %w", err)
	}
	// get the contents
	b, err := p.getBytes(offset+util.INT64_BYTES, len)
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
	buf := make([]byte, util.INT64_BYTES)
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

// constructors
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
	p, err = NewPage(len(b))
	if err != nil {
		return nil, fmt.Errorf("file.Page: NewPageWithBytes: %w", err)
	}
	_, err = p.bb.Seek(0)
	if err != nil {
		return nil, fmt.Errorf("file.Page: NewPageWithBytes: %w", err)
	}
	_, err = p.bb.Write(b)
	if err != nil {
		return nil, fmt.Errorf("file.Page: NewPageWithBytes: %w", err)
	}
	return p, nil
}
