package file

import (
	"fmt"
	"os"
)

type FileMgr struct {
	dbDerectory os.File
	blockSize   int
	isNew       bool
	openFiles   map[string]os.File
}

func NewFileMgr(filename string, blockSize int) *FileMgr {
	isNew := !exists(filename)
	if isNew {
		if err := os.Mkdir(filename, 0777); err != nil {
			fmt.Println(err)
		}
	}
	return &FileMgr{
		blockSize: blockSize,
		isNew:     isNew,
	}
}

func exists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}
