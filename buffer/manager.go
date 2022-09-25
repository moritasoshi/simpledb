package buffer

// The buffer manager allocates several pages, called the "buffer pool"
// Once all buffers are loaded, buffer manager has to replace the unpinned block
// in some candidate buffer to serve new pin request.

import (
	"errors"
	"time"

	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
)

type Manager struct {
	bufferPool   []*Buffer
	numAvailable int
}

var ErrOperationAborted = errors.New("buffer.Manager: operation aborted")

const MAX_TIME = 10 * time.Second // 10 seconds

func NewManager(fm *file.Manager, lm *log.Manager, bufferPoolSize int) *Manager {
	pool := make([]*Buffer, bufferPoolSize)
	for idx := range pool {
		pool[idx] = NewBuffer(fm, lm)
	}
	return &Manager{
		bufferPool:   pool,
		numAvailable: bufferPoolSize,
	}
}

// Pins a buffer to the specified block.
// Potentially waits until a buffer becomes available.
func (bm *Manager) Pin(blk *file.BlockId) (*Buffer, error) {
	start := time.Now()
	buf := bm.tryToPin(blk)
	// Wait until some other unpins.
	for buf == nil && !waitingTooLong(start) {
		// TODO: wait until some other notifies unpinned.
		buf = bm.tryToPin(blk)
	}
	if buf == nil {
		return nil, ErrOperationAborted
	}
	return buf, nil
}

// Unpins and Increases the number of available buffers.
func (bm *Manager) Unpin(buf *Buffer) {
	buf.unpin()
	if !buf.IsPinned() {
		bm.numAvailable++
		// TODO: notify any waiting threads.
	}
}

// Returns the number of available (i.e. unpinned) buffers
func (bm *Manager) Available() int { return bm.numAvailable }

func (bm *Manager) FlushAll(txnum int) {
	for _, buffer := range bm.bufferPool {
		if buffer.ModifyingTx() == txnum {
			buffer.flush()
		}
	}
}

func waitingTooLong(start time.Time) bool {
	return time.Since(start) > MAX_TIME
}

// If there is a buffer assigned to the pool, then Returns that buffer.
// Otherwise, an unpinned buffer from the pool is chosen.
// Returns nil if there are no available buffers.
func (bm *Manager) tryToPin(blk *file.BlockId) *Buffer {
	var buf *Buffer
	buf = bm.findExistingBuffer(blk)
	if buf == nil {
		buf = bm.chooseUnpinnedBuffer()
		if buf == nil {
			return nil
		}
		buf.allocate(blk)
	}
	if !buf.IsPinned() {
		bm.numAvailable--
	}
	buf.pin()
	return buf
}

// Finds the specified block from the buffer pool.
// Returns the target block if exists, or nil if it does not exist.
func (bm *Manager) findExistingBuffer(blk *file.BlockId) *Buffer {
	for _, buf := range bm.bufferPool {
		var b *file.BlockId
		b = buf.Block()
		if b.Equals(blk) {
			return buf
		}
	}
	return nil
}

// Finds a unpinned buffer from the buffer pool.
// Returns a unpinned block if exists, or nil if it does not exist.
func (bm *Manager) chooseUnpinnedBuffer() *Buffer {
	for _, buf := range bm.bufferPool {
		if !buf.IsPinned() {
			return buf
		}
	}
	return nil
}
