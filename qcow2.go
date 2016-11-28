package qcow2

import (
	"fmt"
	"io"
	"log"
	"strconv"
)

// Qcow2 represents a qcow2 file
type Qcow2 interface {
	io.Closer

	Guest() Guest
	ClusterSize() int

	XXX(args ...string)
}

type qcow2 struct {
	header header
}

// Open a qcow2 file
func Open(rw ReaderWriterAt) (Qcow2, error) {
	q := &qcow2{}
	q.header = &headerImpl{}
	if err := q.header.open(rw); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *qcow2) Guest() Guest {
	g := &guestImpl{}
	g.open(q.header, q.refcounts(), q.header.l1Offset(), q.header.size())
	return g
}

func (q *qcow2) ClusterSize() int {
	return q.header.clusterSize()
}

func (q *qcow2) Close() error {
	return q.header.close()
}

func (q *qcow2) refcounts() refcounts {
	r := &refcountsImpl{}
	r.open(q.header)
	return r
}

func (q *qcow2) XXX(args ...string) {
	op, args := args[0], args[1:]

	r := q.refcounts()
	switch op {
	case "refcounts":
		for i := range r.used() {
			if i.err != nil {
				log.Fatal(i.err)
			}
			fmt.Printf("%7d: %2d\n", i.idx, i.rc)
		}
	case "alloc":
		count := 1
		var err error
		if len(args) >= 1 {
			if count, err = strconv.Atoi(args[0]); err != nil {
				log.Fatal(err)
			}
		}
		cluster, err := r.allocate(int64(count))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%7d\n", cluster)
	default:
		log.Fatal("Bad operation")
	}
}
