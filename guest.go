package qcow2

import (
	"bytes"
	"errors"
	"io"
)

type Guest interface {
	ReaderWriterAt
	Size() int64
}

const (
	cow        uint64 = 1 << 63
	compressed uint64 = 1 << 62
	l1Valid    uint64 = cow | (1<<56-1)&^0x1ff
	l2Valid    uint64 = l1Valid | compressed
)

type guestImpl struct {
	ioAt        ioAt
	size        int64
	clusterSize int
	l1_position int64
	l1_clusters int
}

func (g *guestImpl) lookupCluster(idx int) (int64, error) {
	l2Entries := g.clusterSize / 8

	l1Offset := g.l1_position + int64(idx/l2Entries*8)
	l1, err := g.ioAt.read64(l1Offset)
	if err != nil {
		return 0, err
	}
	if l1&^l1Valid != 0 {
		return 0, errors.New("Invalid L1 entry")
	}
	if l1 == 0 {
		return 0, nil
	}

	l2Offset := int64(l1&^cow) + int64((idx%l2Entries)*8)
	l2, err := g.ioAt.read64(l2Offset)
	if err != nil {
		return 0, err
	}
	if l2&^l2Valid != 0 {
		return 0, errors.New("Invalid L2 entry")
	}
	if l2&compressed != 0 {
		return 0, errors.New("Compression not supported")
	}
	if l2 == 0 {
		return 0, nil
	}

	return int64(l2 &^ cow), nil
}

func zeroFill(p []byte) {
	for i := 0; i < len(p); i++ {
		p[i] = 0
	}
}

func (g *guestImpl) readCluster(p []byte, idx int, off int) error {
	clusterStart, err := g.lookupCluster(idx)
	if err != nil {
		return err
	}

	if clusterStart == 0 {
		zeroFill(p)
	} else {
		if _, err := g.ioAt.ReadAt(p, clusterStart+int64(off)); err != nil {
			return err
		}
	}
	return nil
}

func (g *guestImpl) writeCluster(p []byte, idx int, off int) error {
	clusterStart, err := g.lookupCluster(idx)
	if err != nil {
		return err
	}

	if clusterStart == 0 {
		return errors.New("Allocating sectors not yet implemented")
	} else {
		// Do nothing if there are no changes.
		pos := clusterStart + int64(off)
		cmp := make([]byte, len(p))
		if _, err := g.ioAt.ReadAt(cmp, pos); err != nil {
			return err
		}
		if bytes.Compare(p, cmp) == 0 {
			return nil
		}

		if _, err := g.ioAt.WriteAt(p, pos); err != nil {
			return err
		}
	}
	return nil
}

type clusterFunc func(g *guestImpl, p []byte, idx int, off int) error

func (g *guestImpl) perCluster(p []byte, off int64, f clusterFunc) (n int, err error) {
	if off+int64(len(p)) > g.size {
		return 0, io.ErrUnexpectedEOF
	}

	idx := int(off / int64(g.clusterSize))
	offset := int(off % int64(g.clusterSize))
	n = 0
	for len(p) > 0 {
		length := g.clusterSize - offset
		if length > len(p) {
			length = len(p)
		}
		if err = f(g, p[:length], idx, offset); err != nil {
			return
		}
		p = p[length:]
		idx++
		offset = 0
		n += length
	}
	return n, nil
}

func (g *guestImpl) ReadAt(p []byte, off int64) (n int, err error) {
	return g.perCluster(p, off, (*guestImpl).readCluster)
}

func (g *guestImpl) WriteAt(p []byte, off int64) (n int, err error) {
	return g.perCluster(p, off, (*guestImpl).writeCluster)
}

func (g *guestImpl) Size() int64 {
	return g.size
}
