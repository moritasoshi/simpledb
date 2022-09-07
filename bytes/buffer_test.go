package bytes

import (
	"bytes"
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
		buf, _ := NewBuffer(test.bufSize)
		if buf.cap != test.expected {
			t.Errorf("expected %q, got %q", test.expected, buf.cap)
		}
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
