package qcow2

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

type Qcow2 interface {
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
