package file

import (
	"fmt"
	"os"
)

type FileMgr struct {
	blockSize int
}

func NewFileMgr(filename string, blockSize int) *FileMgr {

	if !exists(filename) {
		if err := os.Mkdir(filename, 0777); err != nil {
			fmt.Println(err)
		}
	}
	return &FileMgr{
		blockSize: blockSize,
	}
}

func exists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}
