package qcow2

import (
	"encoding/binary"
	"errors"
	"io"
)

type Guest interface {
	io.ReaderAt
	Size() uint64
}

type guestImpl struct {
	q           *qcow2
	l1_position int64
	l1_clusters int
}

type l1Entry uint64

func (l l1Entry) validate() error {
	if l&0x7f000000000001ff != 0 {
		return errors.New("Bad L1 entry")
	}
	return nil
}
func (l l1Entry) empty() bool {
	return l == 0
}
func (l l1Entry) cow() bool {
	return l&(1<<63) == 0 && !l.empty()
}
func (l l1Entry) offset() int64 {
	return int64(l & (1<<63 - 1))
}

type l2Entry uint64

func (l l2Entry) validate() error {
	if l&0x3f000000000001ff != 0 {
		return errors.New("Bad L2 entry")
	}
	return nil
}
func (l l2Entry) empty() bool {
	return l == 0
}
func (l l2Entry) compressed() bool {
	return l&(1<<62) != 0
}
func (l l2Entry) cow() bool {
	return l&(1<<63) == 0 && !l.empty()
}
func (l l2Entry) offset() int64 {
	return int64(l & (1<<62 - 1))
}

func read64(r io.ReaderAt, off int64) (uint64, error) {
	var buf [8]byte
	if _, err := r.ReadAt(buf[:], off); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func (g *guestImpl) lookupCluster(idx int) (l2Entry, error) {
	l2Entries := g.q.ClusterSize() / 8

	l1Offset := g.l1_position + int64(idx/l2Entries*8)
	v, err := read64(g.q.reader, l1Offset)
	if err != nil {
		return 0, err
	}
	l1 := l1Entry(v)
	if err = l1.validate(); err != nil {
		return 0, err
	}
	if l1.empty() {
		return 0, nil
	}

	l2Offset := l1.offset() + int64((idx%l2Entries)*8)
	v, err = read64(g.q.reader, l2Offset)
	if err != nil {
		return 0, err
	}
	l2 := l2Entry(v)
	if err = l2.validate(); err != nil {
		return 0, err
	}
	return l2, nil
}

func zero(p []byte) {
	for i := 0; i < len(p); i++ {
		p[i] = 0
	}
}

func (g *guestImpl) readCluster(p []byte, idx int, off int) error {
	l2, err := g.lookupCluster(idx)
	if err != nil {
		return err
	}

	if l2.compressed() {
		return errors.New("Compressed clusters unsupported")
	} else if l2.empty() {
		zero(p)
	} else {
		if _, err := g.q.reader.ReadAt(p, l2.offset()+int64(off)); err != nil {
			return err
		}
	}
	return nil
}

func (g *guestImpl) ReadAt(p []byte, off int64) (n int, err error) {
	clusterSize := g.q.ClusterSize()
	idx := int(off / int64(clusterSize))
	offset := int(off % int64(clusterSize))
	n = 0
	for len(p) > 0 {
		length := clusterSize - offset
		if length > len(p) {
			length = len(p)
		}
		if err = g.readCluster(p[:length], idx, offset); err != nil {
			return
		}
		p = p[length:]
		idx++
		offset = 0
		n += length
	}
	return n, nil
}

func (g *guestImpl) Size() uint64 {
	return g.q.header.Size
}
