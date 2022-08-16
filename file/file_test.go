package file

import (
	"fmt"
	"testing"
)

func TestFile(t *testing.T) {
	fm := NewManager("filetest", 400)
	blk := &BlockId{"testfile", 2}
	p1 := NewPage(fm.blockSize)

	pos1 := 88
	p1.SetString(pos1, "abcdefghijklm")
	pos2 := pos1 + p1.MaxLength(len("abcdefghijklm"))
	p1.SetInt(pos2, 345)

	fm.Write(blk, p1)

	p2 := NewPage(fm.blockSize)
	fm.Read(blk, p2)

	fmt.Printf("offset %d contains %d\n", pos2, p2.GetInt(pos2))
	fmt.Printf("offset %d contains %s\n", pos1, p2.GetString(pos1))

	expected := "abcdefghijklm"
	actual := p2.GetString(pos1)

	if expected != actual {
		t.Fatalf("test failed. expected: %s, actual: %s", expected, actual)
	}

}
