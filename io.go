package qcow2

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

// ReaderWriterAt combines positioned reading and writing
type ReaderWriterAt interface {
	io.ReaderAt
	io.WriterAt
}

type ioAt struct {
	ReaderWriterAt
	order binary.ByteOrder
}

func (i *ioAt) readAt(off int64, data interface{}) error {
	section := io.NewSectionReader(i, off, math.MaxInt64-off)
	return binary.Read(section, i.order, data)
}

func (i *ioAt) writeAt(off int64, data interface{}) error {
	var buf bytes.Buffer
	if err := binary.Write(&buf, i.order, data); err != nil {
		return err
	}
	_, err := i.WriteAt(buf.Bytes(), off)
	return err
}

func (i *ioAt) read64(off int64) (uint64, error) {
	var buf [8]byte
	if _, err := i.ReadAt(buf[:], off); err != nil {
		return 0, err
	}
	return i.order.Uint64(buf[:]), nil
}

func (i *ioAt) write64(off int64, v uint64) error {
	var buf [8]byte
	i.order.PutUint64(buf[:], v)
	_, err := i.WriteAt(buf[:], off)
	return err
}

func (i *ioAt) fill(off int64, count int, c byte) error {
	buf := make([]byte, count)
	for i := 0; i < count; i++ {
		buf[i] = c
	}
	_, err := i.WriteAt(buf, off)
	return err
}
