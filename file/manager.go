package file

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Manager struct {
	dirname   string
	blockSize int
	openFiles map[string]*os.File
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
		openFiles: make(map[string]*os.File),
	}
}

func (fm *Manager) Write(b *BlockId, page *Page) {
	f := fm.getFile(b.filename)
	f.Seek(int64(b.blknum*fm.blockSize), io.SeekStart)
	f.Write(page.Contents())
}

func (fm *Manager) Read(b *BlockId, page *Page) {
	f := fm.getFile(b.filename)
	f.Seek(int64((b.blknum)*fm.blockSize), io.SeekStart)
	buf := make([]byte, fm.blockSize)
	f.Read(buf)
	page.setBytes(0, buf)
}

// ファイルを取得
// 存在しなければ作成する
func (fm *Manager) getFile(filename string) *os.File {
	f := fm.openFiles[filename]
	if f != nil {
		return f
	}

	filepath := filepath.Join(fm.dirname, filename)
	f, err := os.OpenFile(filepath, os.O_RDWR, 0755)
	if os.IsNotExist(err) {
		if f, err = os.Create(filepath); err != nil {
			log.Fatal(err)
		}
	}
	fm.openFiles[filename] = f
	return f
}
