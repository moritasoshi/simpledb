package tx

import (
	"errors"
	"sync"
	"time"

	"github.com/moritasoshi/simpledb/file"
)

const maxTime = 10 * time.Second

var ErrLockAborted = errors.New("tx.LockTable: a lock could not be obtained")

type LockTable struct {
	mu    sync.Mutex
	locks map[*file.BlockId]int
}

func NewLockTable() *LockTable { return &LockTable{} }

func (t *LockTable) SLock(blk *file.BlockId) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	start := time.Now()
	for t.hasXLock(blk) && !waitingTooLong(start) {
		// TODO: wait until some other notifies
	}
	if t.hasXLock(blk) {
		return ErrLockAborted
	}
	val := t.getLockVal(blk)
	t.locks[blk] = val + 1
	return nil
}

func (t *LockTable) xLock(blk *file.BlockId) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	start := time.Now()
	for t.hasOtherSLock(blk) && !waitingTooLong(start) {
		// TODO: wait until some other notifies
	}
	if t.hasOtherSLock(blk) {
		return ErrLockAborted
	}
	t.locks[blk] = -1
	return nil
}

func (t *LockTable) unlock(blk *file.BlockId) {
	t.mu.Lock()
	defer t.mu.Unlock()
	val := t.getLockVal(blk)
	if val > 1 {
		t.locks[blk] = val - 1
	} else {
		delete(t.locks, blk)
		// todo: notify other goroutines
	}
}
func (t *LockTable) hasXLock(blk *file.BlockId) bool      { return t.getLockVal(blk) < 0 }
func (t *LockTable) hasOtherSLock(blk *file.BlockId) bool { return t.getLockVal(blk) > 1 }
func (t *LockTable) getLockVal(blk *file.BlockId) int {
	ival, ok := t.locks[blk]
	if ok {
		return ival
	}
	return 0
}
func waitingTooLong(start time.Time) bool { return time.Since(start) > maxTime }
