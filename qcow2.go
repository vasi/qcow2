package qcow2

import (
	"io"

	"github.com/vasi/qcow2/eio"
)

// Qcow2 represents a qcow2 file
type Qcow2 interface {
	io.Closer

	Version() int

	Guest() (Guest, error)
	ClusterSize() int

	Snapshots() ([]Snapshot, error)
}

type qcow2 struct {
	header header
}

// Open a qcow2 file
func Open(rw eio.ReaderWriterAt) (q Qcow2, err error) {
	var qi *qcow2
	err = eio.BacktraceWrap(func() {
		qi = &qcow2{}
		qi.header = &headerImpl{}
		qi.header.open(rw)
	})
	return qi, err
}

func (q *qcow2) Guest() (g Guest, err error) {
	err = eio.BacktraceWrap(func() {
		g = &guestImpl{}
		g.open(q.header, q.refcounts(), q.header.l1Offset(), q.header.size())
	})
	return
}

func (q *qcow2) ClusterSize() int {
	return q.header.clusterSize()
}

func (q *qcow2) Close() error {
	return nil
}

func (q *qcow2) Snapshots() (snaps []Snapshot, err error) {
	err = eio.BacktraceWrap(func() {
		snaps = readSnapshots(q.header)
	})
	return
}

func (q *qcow2) refcounts() refcounts {
	r := &refcountsImpl{}
	r.open(q.header)
	return r
}

func (q *qcow2) Version() int {
	return q.header.version()
}
