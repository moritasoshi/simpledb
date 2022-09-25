package buffer

// A Buffer is the object that contains status information, such as
// whether it is pinned and, if so, what block it is assigned to.

import (
	"fmt"

	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
)

type Buffer struct {
	contents *file.Page
	blk      *file.BlockId
	pins     int // the number of times the page is pinned
	txnum    int // transaction number identifies the transaction
	lsn      int // log sequence number

	fm *file.Manager
	lm *log.Manager
}

func NewBuffer(fm *file.Manager, lm *log.Manager) *Buffer {
	p, err := file.NewPage(fm.BlockSize())
	if err != nil {
		panic(fmt.Errorf("buffer.Buffer: NewBuffer: %w", err))
	}
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: p,
		blk:      nil,
		pins:     0,
		txnum:    -1,
		lsn:      -1,
	}
}

func (buf *Buffer) SetModified(txnum int, lsn int) {
	buf.txnum = txnum
	if lsn > 0 {
		buf.lsn = lsn
	}
}

func (buf *Buffer) ModifyingTx() int     { return buf.txnum }
func (buf *Buffer) Contents() *file.Page { return buf.contents }
func (buf *Buffer) Block() *file.BlockId { return buf.blk }
func (buf *Buffer) IsPinned() bool       { return buf.pins > 0 }

// Increase the buffer's pin count.
func (buf *Buffer) pin() { buf.pins++ }

// Decrease the buffer's pin count.
func (buf *Buffer) unpin() { buf.pins-- }

// Reads the contents of the specified block into the contents of the buffer.
// If the buffer was dirty, then its previous contents are written to disk.
func (buf *Buffer) allocate(b *file.BlockId) {
	buf.flush()
	buf.blk = b
	buf.fm.Read(buf.blk, buf.contents)
	buf.pins = 0
}

// Writes the buffer to its disk block if it is dirty.
func (buf *Buffer) flush() {
	// if the page has not been modified, then not do anything.
	if buf.txnum >= 0 {
		buf.lm.Flush(buf.lsn)
		buf.fm.Write(buf.blk, buf.contents)
		buf.txnum = -1
	}
}
