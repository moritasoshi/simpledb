package buffer

// The buffer manager allocates several pages, called the "buffer pool"
// Once all buffers are loaded, buffer manager has to replace the unpinned block
// in some candidate buffer to serve new pin request.

import (
	"errors"
	"sync"
	"time"

	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
)

type Manager struct {
	unpinned   *sync.Cond
	bufferPool []*Buffer

	// numAvailable is the number of remaining allocatable buffers
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
		unpinned:     sync.NewCond(&sync.Mutex{}),
		bufferPool:   pool,
		numAvailable: bufferPoolSize,
	}
}

// Pins a buffer to the specified block.
// Potentially waits until a buffer becomes available.
func (bm *Manager) Pin(blk *file.BlockId) (*Buffer, error) {
	buf := bm.tryToPin(blk)

	// Wait until some other unpins.
	if buf == nil {
		c := make(chan struct{})
		var goroutineRunning sync.WaitGroup
		goroutineRunning.Add(1)
		go func() {
			goroutineRunning.Done()
			bm.unpinned.L.Lock()
			defer bm.unpinned.L.Unlock()
			bm.unpinned.Wait()
			close(c)
		}()
		goroutineRunning.Wait()
		select {
		case <-c:
			// TODO: run tryToPin() before Unlock
			buf = bm.tryToPin(blk)
		case <-time.After(MAX_TIME):
			buf = nil
		}
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
		// notify any waiting goroutines.
		bm.unpinned.Broadcast()
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

// 対象のブロックがすでにバッファプールに割り当てられていればそのバッファを返す
// 割り当てられていなければ、利用可能なバッファを検索し入れ替える
// 利用可能なバッファがない場合はnilを返す
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
	// 対象のバッファがピン留めされていない=新たに割り当てたバッファなので利用可能数を減らす
	if !buf.IsPinned() {
		bm.numAvailable--
	}
	buf.pin()
	return buf
}

// バッファプールから対象のブロックを割り当てたバッファを取得する
// 対象のブロックが割り当て済みでなければnilを返す
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

// バッファプールの中からピン留めされていないバッファ検索して返す
// 利用可能なバッファがなければnilを返す
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
