package main

import "fmt"

func main() {
	str := "アイウエオ"
	a := []byte(str)
	fmt.Printf("%v: %d", a, len(a))
	fmt.Printf("%v: %d", str, len(str))
}
