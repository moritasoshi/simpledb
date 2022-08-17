package server

import (
// "github.com/moritasoshi/simpledb/file"
)

const (
	blockSize  = 400
	bufferSize = 8
	logFile    = "simpledb.log"
)

type SimpleDB struct {
	// Fm file.FileManager
}

func NewSimpleDB(dirname string, blockSize int, bufferSize int) *SimpleDB {
	return &SimpleDB{
		// Fm: *file.NewFileManager(dirname, blockSize),
	}
}
