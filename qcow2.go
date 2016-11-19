package qcow2

import "io"
import "log"

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
	return &guestImpl{
		q.header.io(),
		q.header.size(),
		q.header.clusterSize(),
		q.header.l1Offset(),
		q.header.l1Size(),
	}
}

func (q *qcow2) ClusterSize() int {
	return q.header.clusterSize()
}

func (q *qcow2) Close() error {
	return q.header.close()
}

func (q *qcow2) XXX() {
	if err := q.header.write(); err != nil {
		log.Fatal(err)
	}
}
