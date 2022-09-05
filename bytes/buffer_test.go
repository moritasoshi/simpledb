package bytes

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

const wantErr, noErr = true, false

func TestByteBuffer(t *testing.T) {
	buf, _ := NewBuffer(400)

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

func Test_NewBuffer(t *testing.T) {
	buf, _ := NewBuffer(100)
	cases := map[string]struct {
		in   int
		want int
	}{
		"offset_0":     {buf.off, 0},
		"capacity_100": {buf.cap, 100},
		"size_100":     {len(buf.buf), 100},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if tt.in != tt.want {
				t.Errorf("actual %v, want %v", tt.in, tt.want)
			}
		})
	}
	t.Run("buffer = [0,0,...,0]", func(t *testing.T) {
		t.Parallel()
		for _, byte := range buf.buf {
			if byte != 0 {
				t.Errorf("actual %v, want %v", byte, 0)
			}
		}
	})
	// Errors
	t.Run("error", func(t *testing.T) {
		_, err := NewBuffer(-1)
		if !errors.Is(err, ErrInvalidBufSize) {
			t.Errorf("actual %v, want %v", err, ErrInvalidBufSize)
		}
	})

}

func Test_NewBufferWithBytes(t *testing.T) {
	buf := NewBufferWithBytes([]byte("hello world"))
	cases := map[string]struct {
		in   int
		want int
	}{
		"offset_0":    {buf.off, 0},
		"capacity_11": {buf.cap, 11},
		"size_11":     {len(buf.buf), 11},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if tt.in != tt.want {
				t.Errorf("actual %v, want %v", tt.in, tt.want)
			}
		})
	}
}

func Test_Seek(t *testing.T) {
	buf, _ := NewBuffer(100)
	cases := map[string]struct {
		in        int
		want      int
		expectErr bool
	}{
		"offset_100":       {100, 100, noErr},
		"error offset_-1":  {-1, 0, wantErr},
		"error offset_101": {101, 0, wantErr},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			off, err := buf.Seek(tt.in)
			if tt.expectErr {
				if err == nil {
					t.Errorf("want error")
				}
			} else {
				if off != tt.want {
					t.Errorf("actual %v, want %v", off, tt.want)
				}
			}
		})
	}
}

func TestWrite(t *testing.T) {
	type args struct {
		buf []byte
		off int
		cnt int
	}
	cases := map[string]struct {
		in        string
		want      args
		expectErr bool
	}{
		"nothing":         {"", args{make([]byte, 10), 0, 0}, noErr},
		"helloworld":      {"helloworld", args{[]byte("helloworld"), 10, 10}, noErr},
		"out of capacity": {"hello world", args{make([]byte, 10), 0, 0}, wantErr},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			buf, _ := NewBuffer(10)
			cnt, err := buf.Write([]byte(tt.in))
			if tt.expectErr {
				if err == nil {
					t.Errorf("want error")
				}
			} else {
				if bytes.Compare(buf.buf, tt.want.buf) != 0 {
					t.Errorf("actual %v, want %v", buf.buf, tt.want.buf)
				}
				if buf.off != tt.want.off {
					t.Errorf("actual %v, want %v", buf.off, tt.want.off)
				}
				if cnt != tt.want.cnt {
					t.Errorf("actual %v, want %v", cnt, tt.want.cnt)
				}
			}
		})
	}
}
