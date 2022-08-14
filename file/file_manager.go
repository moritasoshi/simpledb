package file

import (
	"log"
	"os"
	"path/filepath"
	"strings"
)

type FileManager struct {
	dirname   string
	blockSize int
}

func NewFileManager(dirname string, blockSize int) *FileManager {
	// create directory if not exists
	if _, err := os.Stat(dirname); os.IsNotExist(err) {
		if err := os.Mkdir(dirname, os.ModePerm); err != nil {
			log.Fatal(err)
		}
	}

	// delete temp files
	files, err := os.ReadDir(dirname)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "temp") {
			filepath := filepath.Join(dirname, file.Name())
			os.Remove(filepath)
		}
	}

	return &FileManager{
		dirname:   dirname,
		blockSize: blockSize,
	}
}
