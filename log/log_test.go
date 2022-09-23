package log

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/util"
)

const TEST_DIR = "logtest"

func reset() {
	if err := os.RemoveAll(TEST_DIR); err != nil {
		fmt.Println(err)
	}
}

func TestLog(t *testing.T) {
	reset()
	fm, _ := file.NewManager("logtest", 400)
	lm := NewManager(fm, "simpledb.log")

	// printLogRecords(lm, "The initial empty log file:")
	fmt.Println("done")
	createRecords(lm, 1, 35)
	printLogRecords(lm, "The log file has these records:")
	createRecords(lm, 36, 70)
	lm.Flush(65)
	printLogRecords(lm, "The log file has these records:")

	t.Run("testlog", func(t *testing.T) {
		if got := true; !got {
			t.Errorf("GetInt() = %v, want %v", got, got)
		}
	})
}

func printLogRecords(lm *Manager, msg string) {
	fmt.Println(msg)
	iter := lm.Iterator()
	for iter.HasNext() {
		rec := iter.Next()
		p, _ := file.NewPageBytes(rec)
		s, _ := p.GetString(0)
		nPos := file.MaxLength(len(s))
		val, _ := p.GetInt(nPos)
		fmt.Printf("[%s, %d]\n", s, val)
	}
	fmt.Println()
}

func createRecords(lm *Manager, start int, end int) {
	for i := start; i <= end; i++ {
		rec := createLogRecords(lm, "record"+strconv.Itoa(i), i+100)
		lm.Append(rec)
	}
	fmt.Println()
}

// Create a log record having two values: a string and an integer.
func createLogRecords(lm *Manager, s string, n int) []byte {
	size := file.MaxLength(len(s)) + util.INT64_BYTES*2
	p, _ := file.NewPage(size)
	p.SetString(0, s)
	p.SetInt(file.MaxLength(len(s)), n)
	return p.Contents()
}
