package file

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Manager struct {
	dirname   string
	blockSize int
}

func NewManager(dirname string, blockSize int) *Manager {
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

	return &Manager{
		dirname:   dirname,
		blockSize: blockSize,
	}
}

func (fm *Manager) Write(b *BlockId, page *Page) {
	filepath := filepath.Join(fm.dirname, b.filename)
	f, err := os.OpenFile(filepath, os.O_RDWR, 0755)
	if os.IsNotExist(err) {
		if f, err = os.Create(filepath); err != nil {
			log.Fatal(err)
		}
	}
	f.Seek(int64(b.blknum*fm.blockSize), io.SeekStart)
	io.WriteString(os.Stdout, strconv.Itoa(b.blknum*fm.blockSize))
	f.Write(page.Contents())
}
