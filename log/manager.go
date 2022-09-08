package log

// log management algorithm
// 1. Permanently allocate one memory page to hold the contents of the last block of the log file.
//      Call this page P.
// 2. When a new log record is submitted:
//      a) If there is no room in P, then:
//        Write P to disk and clear its contents.
//      b) Append the new log record to P.
// 3. When the database system requests that a particular log record be written to disk:
//      a) Determine if that log record is in P.
//      b) If so, then write P to disk.

import (
	"log"
	"sync"

	"github.com/moritasoshi/simpledb/file"
)

const INT64_BYTES = 8

type Manager struct {
	mu           sync.Mutex
	fm           *file.Manager
	filename     string
	page         *file.Page
	currentBlock *file.BlockId
	latestLSN    int // Long Sequence Number
	lastSavedLSN int
}

func NewManager(fm *file.Manager, filename string) *Manager {
	var blk *file.BlockId
	p, err := file.NewPage(fm.BlockSize())
	if err != nil {
		log.Fatal(err)
	}
	size := fm.CountBlocks(filename)
	if size == 0 {
		p.SetInt(0, fm.BlockSize())
		blk = file.NewBlockId(filename, 0)
		fm.Write(blk, p)
	} else {
		blk = file.NewBlockId(filename, size-1)
		fm.Read(blk, p)
	}
	return &Manager{
		fm:           fm,
		filename:     filename,
		page:         p,
		currentBlock: blk,
	}
}

func (lm *Manager) Iterator() *Iterator {
	lm.flush()
	return NewIterator(lm.fm, lm.currentBlock)
}

// Append appends a log record to the log buffer.
// Log records are written right to left in the buffer.
// The size of the record is written before the bytes.
// The beginning of the buffer contains the location of the last-written record (the "boundary").
// Storing the records backwards makes it easy to read them in reverse order.
func (lm *Manager) Append(rec []byte) int {
	boundary, _ := lm.page.GetInt(0)
	bytesNeeded := len(rec) + INT64_BYTES
	if boundary-bytesNeeded < INT64_BYTES {
		lm.flush()
		lm.currentBlock = lm.AppendNewBlock()
		boundary, _ = lm.page.GetInt(0)
	}
	pos := boundary - bytesNeeded

	lm.page.SetBytes(pos, rec)
	lm.page.SetInt(0, pos)
	lm.latestLSN += 1
	return lm.latestLSN
}

func (lm *Manager) Flush(lsn int) {
	if lsn >= lm.lastSavedLSN {
		lm.flush()
	}
}

// Initialize the bytebuffer and append it to the log file.
func (lm *Manager) AppendNewBlock() *file.BlockId {
	blk := lm.fm.Append(lm.filename)
	lm.page.SetInt(0, lm.fm.BlockSize())
	lm.fm.Write(blk, lm.page)
	return blk
}

// Write the buffer to the log file.
func (lm *Manager) flush() {
	lm.fm.Write(lm.currentBlock, lm.page)
	lm.lastSavedLSN = lm.latestLSN
}
