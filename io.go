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

const bufSize int = 32 * 1024

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

func (i *ioAt) makeBuf(max int) []byte {
	if max > bufSize {
		max = bufSize
	}
	return make([]byte, max)
}
func (i *ioAt) truncBuf(buf []byte, max int) []byte {
	if max < len(buf) {
		return buf[:max]
	}
	return buf
}

func (i *ioAt) fill(off int64, count int, c byte) error {
	buf := i.makeBuf(count)
	for i := 0; i < len(buf); i++ {
		buf[i] = c
	}

	for count > 0 {
		buf = i.truncBuf(buf, count)
		if _, err := i.WriteAt(buf, off); err != nil {
			return err
		}
		count -= len(buf)
		off += int64(len(buf))
	}

	return nil
}

func (i *ioAt) copy(dst int64, src int64, count int) error {
	buf := i.makeBuf(count)
	for count > 0 {
		buf = i.truncBuf(buf, count)
		if _, err := i.ReadAt(buf, src); err != nil {
			return err
		}
		if _, err := i.WriteAt(buf, dst); err != nil {
			return err
		}
		count -= len(buf)
		src += int64(len(buf))
		dst += int64(len(buf))
	}
	return nil
}
