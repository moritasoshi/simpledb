package log

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/moritasoshi/simpledb/file"
)

func TestLog(t *testing.T) {
	fm, _ := file.NewManager("logtest", 400)
	lm := NewManager(fm, "simpledb.log")

	printLogRecords(lm, "The initial empty log file:")
	fmt.Println("done")
	createRecords(lm, 1, 35)

	t.Run("testlog", func(t *testing.T) {
		if got := true; !got {
			t.Errorf("GetInt() = %v, want %v", got, got)
		}
	})
}

func printLogRecords(lm *Manager, msg string) {
	fmt.Println(msg)
	iter := lm.Iterator()
	for iter.hasNext() {
		rec := iter.next()
		p, _ := file.NewPageBytes(rec)
		s, _ := p.GetString(0)
		nPos := file.MaxLength(len(s))
		val, _ := p.GetInt(nPos)
		fmt.Printf("[%s, %d]\n", s, val)
	}
	fmt.Println()
}

func createRecords(lm *Manager, start int, end int) {
	fmt.Print("Creating records: ")
	for i := start; i <= end; i++ {
		rec := createLogRecords(lm, "record"+strconv.Itoa(i), i+100)
		lsn := lm.Append(rec)
		fmt.Print(lsn, " ")
	}
	fmt.Println()
}

// Create a log record having two values: a string and an integer.
func createLogRecords(lm *Manager, s string, n int) []byte {
	size := file.MaxLength(len(s)) + INT64_BYTES
	b := make([]byte, size)
	p, _ := file.NewPage(size)
	p.SetString(0, s)
	p.SetInt(file.MaxLength(len(s)), n)
	return b
}
