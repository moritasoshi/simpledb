package file

import (
	"reflect"
	"testing"
)

func TestFile(t *testing.T) {
	fm := NewFileMgr("filetest", 400)
	blockid := BlockId{"testfile", 2}
	pos1 := 88

	p1 := NewPage(fm.blockSize)

	if reflect.TypeOf(fm) != reflect.TypeOf(1) {
		t.Fatal("failed test")
	}
}
