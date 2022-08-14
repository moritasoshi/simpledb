package file

import (
	"fmt"
	"testing"
)

func TestFile(t *testing.T) {
	fm := NewFileMgr("filetest", 400)
	// blk := BlockId{"testfile", 2}
	p1 := NewPage(fm.blockSize)

	pos1 := 88
	p1.SetString(pos1, "abcdefghijklm")

	pos2 := pos1 + p1.MaxLength(len("abcdefghijklm"))
	p1.SetInt64(pos2, 345)

	// fm.Write(blk)
	//
	// p2 := NewPage(fm.blockSize)
	// fm.Read(blk, p2)

	fmt.Printf("offset %d contains %d\n", pos2, p1.GetInt64(pos2))
	fmt.Printf("offset %d contains %s\n", pos1, p1.GetString(pos1))

	expected := "abcdefghijklm"
	actual := p1.GetString(pos1)

	if expected != actual {
		t.Fatalf("test failed. expected: %s, actual: %s", expected, actual)
	}

}
