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

// a block(page) and log records
//
// ------------------------------------------------------------------------------------
// | block size (400 bytes)                                                           |
// ------------------------------------------------------------------------------------
// |                     |                                      ||                    |
// | boundary (16 bytes) | .................................... || record1 (20 bytes) |
// |                     |                                      ||                    |
// ------------------------------------------------------------------------------------
//
// boundary = 400 - 20 =  380

import (
	"log"
	"sync"

	"github.com/moritasoshi/simpledb/file"
)

const INT64_BYTES = 8
const BOUNDARY_BYTES = INT64_BYTES * 2

type Manager struct {
	mu           sync.Mutex
	fm           *file.Manager
	filename     string
	page         *file.Page
	currentBlock *file.BlockId
	// latestLSN identifies the new log record.
	latestLSN int
	// lastSavedLSN identifies the log record already saved to disk.
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

// Append appends a log record to the log buffer.
// Log records are written right to left in the buffer.
// The size of the record is written before the bytes.
// The beginning of the buffer contains the location of the last-written record (the "boundary").
// Storing the records backwards makes it easy to read them in reverse order.
func (lm *Manager) Append(rec []byte) int {
	boundary, _ := lm.page.GetInt(0)
	len := file.MaxLength(len(rec))
	// no capacity, so create a new one.
	if boundary-len < BOUNDARY_BYTES {
		lm.flush()
		p, err := file.NewPage(lm.fm.BlockSize())
		if err != nil {
			log.Fatal(err)
		}
		lm.page = p
		lm.currentBlock = lm.AppendNewBlock()
		boundary, _ = lm.page.GetInt(0)
	}
	pos := boundary - len
	lm.page.SetBytes(pos, rec)
	// set the boundary
	lm.page.SetInt(0, pos)
	lm.latestLSN += 1
	return lm.latestLSN
}

// Initialize the bytebuffer and append it to the log file.
func (lm *Manager) AppendNewBlock() *file.BlockId {
	blk := lm.fm.Append(lm.filename)
	// set the boundary on the head of a log record.
	lm.page.SetInt(0, lm.fm.BlockSize())
	lm.fm.Write(blk, lm.page)
	return blk
}

func (lm *Manager) Flush(lsn int) {
	if lsn >= lm.lastSavedLSN {
		lm.flush()
	}
}

func (lm *Manager) Iterator() *Iterator {
	lm.flush()
	return NewIterator(lm.fm, lm.currentBlock)
}

// Write the buffer to the log file.
func (lm *Manager) flush() {
	lm.fm.Write(lm.currentBlock, lm.page)
	lm.lastSavedLSN = lm.latestLSN
}
