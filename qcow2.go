package qcow2

import (
	"fmt"
	"io"
	"log"
)

type Qcow2 interface {
	io.Closer

	Guest() Guest
	ClusterSize() int

	XXX()
}

type qcow2 struct {
	header header
}

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
	g.Open(q.header, q.header.l1Offset(), q.header.size())
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

func (q *qcow2) XXX() {
	r := q.refcounts()
	var i int64
	for i = 0; i < r.max(); i++ {
		rc, err := r.refcount(i)
		if err != nil {
			log.Fatal(err)
		}
		if rc == 0 {
			fmt.Printf("%7d: %2d\n", i, rc)
		}
	}
}
