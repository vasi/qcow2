package qcow2

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

type Qcow2 interface {
	Guest() Guest
	ClusterSize() int
}

type qcow2 struct {
	header headerV2
	reader io.ReaderAt
}

func New(r io.ReaderAt) (Qcow2, error) {
	q := &qcow2{
		reader: r,
	}
	sect := io.NewSectionReader(r, 0, math.MaxInt64)
	if err := binary.Read(sect, binary.BigEndian, &q.header); err != nil {
		return nil, err
	}
	if q.header.Magic != magic {
		return nil, errors.New("not a qcow2 file")
	}

	return q, nil
}

func (q *qcow2) Guest() Guest {
	return &guestImpl{q, int64(q.header.L1TableOffset), int(q.header.L1Size)}
}

func (q *qcow2) ClusterSize() int {
	return 1 << q.header.ClusterBits
}
