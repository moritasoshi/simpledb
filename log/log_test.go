package log_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/moritasoshi/simpledb/file"
	"github.com/moritasoshi/simpledb/log"
	"github.com/moritasoshi/simpledb/util"
)

const TEST_DIR = "logtest"

func reset() {
	if err := os.RemoveAll(TEST_DIR); err != nil {
		fmt.Println(err)
	}
}

func _TestLog(t *testing.T) {
	reset()
	fm, _ := file.NewManager("logtest", 400)
	lm := log.NewManager(fm, "simpledb.log")

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

func printLogRecords(lm *log.Manager, msg string) {
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

func createRecords(lm *log.Manager, start int, end int) {
	for i := start; i <= end; i++ {
		rec := createLogRecords(lm, "record"+strconv.Itoa(i), i+100)
		lm.Append(rec)
	}
	fmt.Println()
}

// Create a log record having two values: a string and an integer.
func createLogRecords(lm *log.Manager, s string, n int) []byte {
	size := file.MaxLength(len(s)) + util.INT64_BYTES*2
	p, _ := file.NewPage(size)
	p.SetString(0, s)
	p.SetInt(file.MaxLength(len(s)), n)
	return p.Contents()
}

var str1 = "abcdefghijklmnopqrstuvwxyz"

func TestAppend(t *testing.T) {
	reset()
	fm, _ := file.NewManager("logtest", 400)
	lm := log.NewManager(fm, "simpledb.log")

	s := file.MaxLength(len(str1))
	p, _ := file.NewPage(s)
	p.SetString(0, str1)
	for i := 0; i < 100; i++ {
		lm.Append(p.Contents())
	}

	iter := lm.Iterator()
	counter := 0
	for iter.HasNext() {
		rec := iter.Next()
		p, _ := file.NewPageBytes(rec)
		s, _ := p.GetString(0)
		fmt.Println(s)
		if s != str1 {
			t.Errorf("want %v got %v", str1, s)
		}
		counter++
	}
	if counter != 100 {
		t.Errorf("want %v got %v", 100, counter)
	}

}
