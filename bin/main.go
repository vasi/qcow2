package main

import (
	"log"
	"os"

	"github.com/vasi/qcow2"
)

func main() {
	f, err := os.OpenFile(os.Args[1], os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	q, err := qcow2.Open(f)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	q.XXX()
}
