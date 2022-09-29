package tx

import (
	"github.com/moritasoshi/simpledb/file"
)

type locktype string

const (
	sLock locktype = "S"
	xLock locktype = "X"
)

type ConcurrentManager struct {
	locks   map[*file.BlockId]locktype
	lockTbl *LockTable
}

func NewConcurrentManager() *ConcurrentManager {
	return &ConcurrentManager{
		locks:   make(map[*file.BlockId]locktype),
		lockTbl: NewLockTable(),
	}
}

func (m *ConcurrentManager) clear() { m.locks = make(map[*file.BlockId]locktype) }

func (m *ConcurrentManager) SLock(blk *file.BlockId) {
	_, exists := m.locks[blk]
	if !exists {
		m.lockTbl.SLock(blk)
		m.locks[blk] = sLock
	}
}
func (m *ConcurrentManager) XLock(blk *file.BlockId) {
	if !m.hasXLock(blk) {
		m.SLock(blk)
		m.lockTbl.xLock(blk)
		m.locks[blk] = xLock

	}
}

func (m *ConcurrentManager) Release() {
	for blk := range m.locks {
		m.lockTbl.unlock(blk)
	}
	m.clear()
}

func (m *ConcurrentManager) hasXLock(blk *file.BlockId) bool {
	ltype, ok := m.locks[blk]
	return ok && ltype == xLock
}
