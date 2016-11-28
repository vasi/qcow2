package qcow2

import "io"

// Qcow2 represents a qcow2 file
type Qcow2 interface {
	io.Closer

	Guest() Guest
	ClusterSize() int
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
