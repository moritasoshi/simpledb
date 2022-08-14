package core

import (
	"fmt"
	"testing"
)

func TestByteBuffer(t *testing.T) {
	buf := NewByteBuffer(400)

	buf.Seek(0)
	buf.Write([]byte("abcdef"))

	act := make([]byte, len([]byte("abcdef")))
	buf.Seek(0)
	buf.Read(act)

	fmt.Printf("expect: %s\n", []byte("abcdef"))
	fmt.Printf("act:    %s\n", act)
	if string(act) != "abcdef" {
		t.Fatalf("failed test\n")
	}
}
