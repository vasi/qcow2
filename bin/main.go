package main

import (
	"fmt"
	"os"

	"github.com/k0kubun/pp"
	"github.com/vasi/qcow2"
)

func main() {
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	q, err := qcow2.New(file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	} else {
		pp.Print(q)
	}
}
