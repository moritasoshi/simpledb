package file

import (
	"errors"
	"testing"

	"github.com/moritasoshi/simpledb/bytes"
)

func TestNewPage(t *testing.T) {
	cases := map[string]struct {
		in        int
		want      error
		expectErr bool
	}{
		"error_-1": {-1, bytes.ErrInvalidBufSize, wantErr},
		"0":        {0, nil, noErr},
		"10":       {10, nil, noErr},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			p, err := NewPage(tt.in)
			if tt.expectErr {
				if !errors.Is(err, tt.want) {
					t.Errorf("actual %v, want %v", errors.Unwrap(err), tt.want)
				}
			} else {
				if p == nil {
					t.Errorf("actual %v", p)
				}
			}
		})
	}
}
func TestSetString(t *testing.T) {
	const pageSize = 10
	type args struct {
		offset int
		val    string
	}
	cases := map[string]struct {
		in        args
		expectErr bool
	}{"ab": {args{0, "ab"}, noErr},
		"12": {args{0, "12"}, noErr},
		"*$": {args{0, "*$"}, noErr},
		"error_overflow_for_head8_and_body3bytes": {args{0, "abc"}, wantErr},
		"error_overflow_for_offset":               {args{1, "ab"}, wantErr},
		"error_offset_out_of_range":               {args{11, "a"}, wantErr},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			p, _ := NewPage(pageSize)
			err := p.SetString(tt.in.offset, tt.in.val)
			if tt.expectErr {
				if err == nil {
					t.Errorf("actual %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("should not failed. %v", err)
				}
			}
		})
	}
}

func TestGetString(t *testing.T) {
	var getStringTests = []struct {
		buffer   string
		offset   int
		expected string
		err      error
	}{
		{"buffer", 0, "buffer", nil},
		// {"buffer", 6, "buffer", nil},
	}
	for _, test := range getStringTests {
		p, _ := NewPageWithBytes([]byte(test.buffer))
		var str string
		str = p.GetString(test.offset)
		if str != test.expected {
			t.Errorf("expected %q, got %q", test.expected, str)
		}
	}
}
