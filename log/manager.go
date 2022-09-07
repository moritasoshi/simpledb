package log

import (
	"log"
	"sync"

	"github.com/moritasoshi/simpledb/file"
)

const INT64_BYTES = 8

type Manager struct {
	mu           sync.Mutex
	fm           *file.Manager
	logFile      string
	logPage      *file.Page
	currentBlock *file.BlockId
	latestLSN    int // Long Sequence Number
	lastSavedLSN int
}

func NewManager(fm *file.Manager, filename string) *Manager {
	var blk *file.BlockId
	page, err := file.NewPage(fm.BlockSize())
	if err != nil {
		log.Fatal(err)
	}
	size := fm.CountBlocks(filename)
	if size == 0 {
		page.SetInt(0, fm.BlockSize())
		blk = file.NewBlockId(filename, 0)
		fm.Write(blk, page)
	} else {
		blk = file.NewBlockId(filename, size-1)
		fm.Read(blk, page)
	}
	return &Manager{
		fm:           fm,
		logFile:      filename,
		logPage:      page,
		currentBlock: blk,
	}
}

func (lm *Manager) Iterator() *Iterator {
	lm.flush()
	return NewIterator(lm.fm, lm.currentBlock)
}

// Appends a log record to the log buffer.
// The record consists of an arbitrary array of bytes.
// Log records are written right to left in the buffer.
// The size of the record is written before the bytes.
// The beginning of the buffer contains the location
// of the last-written record (the "boundary").
// Storing the records backwards makes it easy to read them in reverse order.
func (lm *Manager) Append(logRec []byte) int {
	boundary, _ := lm.logPage.GetInt(0)
	recSize := len(logRec)
	bytesNeeded := recSize + INT64_BYTES
	if boundary-bytesNeeded < INT64_BYTES {
		lm.flush()
		lm.currentBlock = lm.AppendNewBlock()
		boundary, _ = lm.logPage.GetInt(0)
	}
	recPos := boundary - bytesNeeded

	lm.logPage.SetBytes(recPos, logRec)
	lm.logPage.SetInt(0, recPos)
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
	blk := lm.fm.Append(lm.logFile)
	lm.logPage.SetInt(0, lm.fm.BlockSize())
	lm.fm.Write(blk, lm.logPage)
	return blk
}

// Write the buffer to the log file.
func (lm *Manager) flush() {
	lm.fm.Write(lm.currentBlock, lm.logPage)
	lm.lastSavedLSN = lm.latestLSN
}
