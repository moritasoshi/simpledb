package file

import (
	"errors"
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

func NewManager(dirname string, blockSize int) (*Manager, error) {
	if blockSize < 0 {
		return nil, errors.New("file.Manager: NewManager: block size must be greater than 0")
	}
	if len(dirname) <= 0 {
		return nil, errors.New("file.Manager: NewManager: invalid filename")
	}

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
			if err := os.Remove(filepath); err != nil {
				log.Fatal(err)
			}
		}
	}

	return &Manager{
		dirname:   dirname,
		blockSize: blockSize,
		openFiles: make(map[string]*os.File),
	}, nil
}

// Write writes the contents of page into block.
func (fm *Manager) Write(b *BlockId, page *Page) {
	f := fm.getFile(b.filename)
	f.Seek(int64(b.blknum*fm.blockSize), io.SeekStart)
	f.Write(page.Contents())
}

// Read reads the contents of block into page.
func (fm *Manager) Read(b *BlockId, page *Page) {
	f := fm.getFile(b.filename)
	buf := make([]byte, fm.blockSize)
	f.Seek(int64((b.blknum)*fm.blockSize), io.SeekStart)
	f.Read(buf)
	page.setBytes(0, buf)
}

// Append a new block to the file.
func (fm *Manager) Append(filename string) *BlockId {
	newBlkNum := fm.CountBlocks(filename)
	blk := NewBlockId(filename, newBlkNum)
	b := make([]byte, fm.blockSize)

	f := fm.getFile(blk.filename)
	_, err := f.Seek(int64((blk.blknum)*fm.blockSize), io.SeekStart)
	if err != nil {
		log.Fatal("file.Manager: Append: %w", err)
	}
	_, err = f.Write(b)
	if err != nil {
		log.Fatal("file.Manager: Append: %w", err)
	}
	return blk
}

// Count blocks in the file.
func (fm *Manager) CountBlocks(filename string) int {
	f := fm.getFile(filename)
	fi, err := f.Stat()
	if err != nil {
		log.Fatal("Cannot get file information: ", filename)
	}
	return int(fi.Size()) / fm.blockSize
}

func (fm *Manager) BlockSize() int {
	return fm.blockSize
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
