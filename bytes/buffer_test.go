package bytes_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/moritasoshi/simpledb/bytes"
)

const wantErr, noErr = true, false

func TestByteBuffer(t *testing.T) {
	buf, _ := bytes.NewBuffer(400)

	buf.Seek(0)
	buf.Write([]byte("abcdef"))

	act := make([]byte, len([]byte("abcdef")))
	buf.Seek(0)
	buf.Read(act)

	if string(act) != "abcdef" {
		t.Errorf("expected %q, got %q", "abcdef", string(act))
	}
}

func TestNewBuffer(t *testing.T) {
	tests := []struct {
		bufSize  int
		expected int
		err      error
	}{
		{0, 0, nil},
		{100, 100, nil},
	}
	for _, test := range tests {
		buf, _ := bytes.NewBuffer(test.bufSize)
		if buf.Cap() != test.expected {
			t.Errorf("expected %q, got %q", test.expected, buf.Cap())
		}
	}
}

func TestSeek(t *testing.T) {
	buf, _ := bytes.NewBuffer(100)
	cases := []struct {
		in        int
		want      int
		expectErr bool
	}{
		{100, 100, noErr},
		{-1, 0, wantErr},
		{101, 0, wantErr},
	}
	for _, c := range cases {
		off, err := buf.Seek(c.in)
		if c.expectErr {
			if err == nil {
				t.Errorf("want error")
			}
		} else {
			if off != c.want {
				t.Errorf("actual %v, want %v", off, c.want)
			}
		}
	}
}

func testWrite(t *testing.T) {
	buf, _ := bytes.NewBuffer(10)
	buf.Write([]byte("helloworld"))
	str := buf.String()
	if str != "helloworld" {
		t.Errorf("expected %v, got %v", "helloworld", str)
	}
}

func TestWriteErr(t *testing.T) {
	buf, _ := bytes.NewBuffer(10)
	_, err := buf.Write([]byte("hello world"))
	if err != bytes.ErrBufferOverflow {
		t.Errorf("expected %v, got %v", bytes.ErrBufferOverflow, err)
	}

}

func TestStringNumber(t *testing.T) {
	b := "12345"
	buf, _ := bytes.NewBuffer(5)
	_, _ = buf.Write([]byte(b))
	buf.Seek(0)
	if v := buf.String(); v != "12345" {
		fmt.Println(v == "12345", strings.Compare(v, "12345"))
		fmt.Println([]byte(v), []byte("12345"))
		t.Errorf("expected %v, got %v", "12345", v)
	}
}
