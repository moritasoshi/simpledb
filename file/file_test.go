package file

import (
	"fmt"

	"testing"
)

const wantErr, noErr = true, false

func TestFile(t *testing.T) {
	fm, _ := NewManager("filetest", 400)
	blk := NewBlockId("testfile", 2)
	p1, _ := NewPage(fm.blockSize)

	pos1 := 88
	p1.SetString(pos1, "abcdefghijklm")
	pos2 := pos1 + MaxLength(len("abcdefghijklm"))
	p1.SetInt(pos2, 345)

	fm.Write(blk, p1)

	p2, _ := NewPage(fm.blockSize)
	fm.Read(blk, p2)

	i, _ := p2.GetInt(pos2)
	s, _ := p2.GetString(pos1)
	fmt.Printf("offset %d contains %d\n", pos2, i)
	fmt.Printf("offset %d contains %s\n", pos1, s)

	type args struct {
		page     *Page
		position int
	}
	testInt := []struct {
		name string
		args args
		want int
	}{
		{
			name: "GetInt() on read",
			args: args{page: p2, position: pos2},
			want: 345,
		},
		{
			name: "GetInt() on written",
			args: args{page: p1, position: pos2},
			want: 345,
		},
	}
	for _, tt := range testInt {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := tt.args.page.GetInt(tt.args.position); got != tt.want {
				t.Errorf("GetInt() = %v, want %v", got, tt.want)
			}
		})
	}

	testStr := []struct {
		name string
		args args
		want string
	}{
		{
			name: "GetString() on read",
			args: args{page: p2, position: pos1},
			want: "abcdefghijklm",
		},
		{
			name: "GetString() on written",
			args: args{page: p1, position: pos1},
			want: "abcdefghijklm",
		},
	}
	for _, tt := range testStr {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := tt.args.page.GetString(tt.args.position); got != tt.want {
				t.Errorf("GetString() = %v, want %v", got, tt.want)
			}
		})
	}
}
