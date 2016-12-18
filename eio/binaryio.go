package eio

// Binary data reading and writing, using exceptions

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/timtadh/data-structures/exc"
)

const (
	// BufferSize is the buffer size for large operations
	BufferSize int = 32 * 1024
)

// ReaderWriterAt combines positioned reading and writing
type ReaderWriterAt interface {
	io.ReaderAt
	io.WriterAt
}

// BinaryIO allows I/O on binary data.
type BinaryIO struct {
	base  ReaderWriterAt
	order binary.ByteOrder
}

// NewIO constructs a new BinaryIO
func NewIO(base ReaderWriterAt, order binary.ByteOrder) *BinaryIO {
	return &BinaryIO{base, order}
}

// ByteOrder returns the byte order
func (bio *BinaryIO) ByteOrder() binary.ByteOrder {
	return bio.order
}

// ReadAt reads data at an offset
func (bio *BinaryIO) ReadAt(off int64, buf []byte) {
	_, err := bio.base.ReadAt(buf, off)
	exc.ThrowOnError(err)
}

// ReadData reads binary data
func (bio *BinaryIO) ReadData(off int64, data interface{}) {
	sr := NewSequentialReader(bio, off)
	sr.ReadData(data)
}

// WriteAt writes data at an offset
func (bio *BinaryIO) WriteAt(off int64, buf []byte) {
	_, err := bio.base.WriteAt(buf, off)
	exc.ThrowOnError(err)
}

// ReadUint64 reads a 64-bit integer
func (bio *BinaryIO) ReadUint64(off int64) uint64 {
	var buf [8]byte
	bio.ReadAt(off, buf[:])
	return bio.order.Uint64(buf[:])
}

// ReadUint32 reads a 32-bit integer
func (bio *BinaryIO) ReadUint32(off int64) uint32 {
	var buf [4]byte
	bio.ReadAt(off, buf[:])
	return bio.order.Uint32(buf[:])
}

// ReadUint16 reads a 16-bit integer
func (bio *BinaryIO) ReadUint16(off int64) uint16 {
	var buf [2]byte
	bio.ReadAt(off, buf[:])
	return bio.order.Uint16(buf[:])
}

// ReadUint8 reads an 8-bit integer
func (bio *BinaryIO) ReadUint8(off int64) uint8 {
	var buf [1]byte
	bio.ReadAt(off, buf[:])
	return buf[0]
}

// WriteUint64 writes a 64-bit integer.
func (bio *BinaryIO) WriteUint64(off int64, value uint64) {
	var buf [8]byte
	bio.order.PutUint64(buf[:], value)
	bio.WriteAt(off, buf[:])
}

// WriteUint32 writes a 32-bit integer.
func (bio *BinaryIO) WriteUint32(off int64, value uint32) {
	var buf [4]byte
	bio.order.PutUint32(buf[:], value)
	bio.WriteAt(off, buf[:])
}

// WriteUint16 writes a 16-bit integer.
func (bio *BinaryIO) WriteUint16(off int64, value uint16) {
	var buf [2]byte
	bio.order.PutUint16(buf[:], value)
	bio.WriteAt(off, buf[:])
}

// WriteUint8 writes an 8-bit integer.
func (bio *BinaryIO) WriteUint8(off int64, value uint8) {
	buf := []byte{value}
	bio.WriteAt(off, buf[:])
}

// Make a buffer for a large operation
func makeBuf(max int) []byte {
	if max > BufferSize {
		max = BufferSize
	}
	return make([]byte, max)
}

// Truncate a buffer if necesasry, before part of a large operation
func truncBuf(buf []byte, max int) []byte {
	if max < len(buf) {
		return buf[:max]
	}
	return buf
}

// Zero fills a range with zero bytes
func (bio *BinaryIO) Zero(off int64, count int) {
	buf := makeBuf(count)
	for count > 0 {
		buf = truncBuf(buf, count)
		bio.WriteAt(off, buf)
		count -= len(buf)
		off += int64(len(buf))
	}
}

// Copy copies a range of bytes within this BinaryIO
func (bio *BinaryIO) Copy(dst int64, src int64, count int) error {
	buf := makeBuf(count)
	for count > 0 {
		buf = truncBuf(buf, count)
		bio.ReadAt(src, buf)
		bio.WriteAt(dst, buf)
		count -= len(buf)
		src += int64(len(buf))
		dst += int64(len(buf))
	}
	return nil
}

// SequentialReader allows sequential reads of binary data.
type SequentialReader struct {
	bio    *BinaryIO
	offset int64
	reader *io.SectionReader
}

// NewSequentialReader returns a SequentialReader that reads from io
func NewSequentialReader(bio *BinaryIO, off int64) *SequentialReader {
	return NewReaderSection(bio, off, math.MaxInt64-off)
}

// NewReaderSection returns a SequentialReader with a maximum size
func NewReaderSection(bio *BinaryIO, off int64, size int64) *SequentialReader {
	return &SequentialReader{
		bio:    bio,
		offset: off,
		reader: io.NewSectionReader(bio.base, off, size),
	}
}

// SubReader returns a new SequentialReader that refers to a subset of this one
func (sr *SequentialReader) SubReader(size int64) *SequentialReader {
	r := NewReaderSection(sr.bio, sr.offset+sr.Position(), size)
	sr.Skip(size)
	return r
}

// Remain gets the size of the remaining data in this reader
func (sr *SequentialReader) Remain() int64 {
	return sr.reader.Size() - sr.Position()
}

// ReadBuf reads a byte buffer
func (sr *SequentialReader) ReadBuf(buf []byte) {
	_, err := io.ReadFull(sr.reader, buf)
	exc.ThrowOnError(err)
}

// ReadNewBuf reads a new byte buffer
func (sr *SequentialReader) ReadNewBuf(size int) []byte {
	buf := make([]byte, size)
	sr.ReadBuf(buf)
	return buf
}

// ReadData reads a data structure
func (sr *SequentialReader) ReadData(data interface{}) {
	exc.ThrowOnError(binary.Read(sr.reader, sr.bio.order, data))
}

// ReadUint64 reads a 64-bit integer
func (sr *SequentialReader) ReadUint64() uint64 {
	var v uint64
	sr.ReadData(&v)
	return v
}

// ReadUint32 reads a 32-bit integer
func (sr *SequentialReader) ReadUint32() uint32 {
	var v uint32
	sr.ReadData(&v)
	return v
}

// ReadUint16 reads a 16-bit integer
func (sr *SequentialReader) ReadUint16() uint16 {
	var v uint16
	sr.ReadData(&v)
	return v
}

// ReadUint8 reads a byte
func (sr *SequentialReader) ReadUint8() uint8 {
	var v byte
	sr.ReadData(&v)
	return v
}

// Skip skips ahead
func (sr *SequentialReader) Skip(n int64) {
	_, err := sr.reader.Seek(n, io.SeekCurrent)
	exc.ThrowOnError(err)
}

// Align skips until the position is aligned by n
func (sr *SequentialReader) Align(n int) {
	pos := sr.Position()
	nn := int64(n)
	if pos%nn != 0 {
		sr.Skip(nn - pos%nn)
	}
}

// Position gets the current offset
func (sr *SequentialReader) Position() int64 {
	pos, err := sr.reader.Seek(0, io.SeekCurrent)
	exc.ThrowOnError(err)
	return pos
}

// BinaryWriter allows writing binary data
type BinaryWriter struct {
	bio *BinaryIO
	off int64
	buf bytes.Buffer
}

// NewBinaryWriter creates a new binary writer
func NewBinaryWriter(bio *BinaryIO, off int64) *BinaryWriter {
	return &BinaryWriter{bio, off, bytes.Buffer{}}
}

// Size gets the size of the uncommitted data
func (bw *BinaryWriter) Size() int {
	return bw.buf.Len()
}

// WriteBuf writes a buffer
func (bw *BinaryWriter) WriteBuf(buf []byte) {
	bw.buf.Write(buf)
}

// WriteData writes binary data
func (bw *BinaryWriter) WriteData(data interface{}) {
	exc.ThrowOnError(binary.Write(&bw.buf, bw.bio.order, data))
}

// Align skips until the position is aligned by n
func (bw *BinaryWriter) Align(n int) {
	len := bw.buf.Len()
	if len%n != 0 {
		buf := make([]byte, n-len%n)
		bw.buf.Write(buf)
	}
}

// Commit actually writes the data to the underlying BinaryIO
func (bw *BinaryWriter) Commit() {
	bw.bio.WriteAt(bw.off, bw.buf.Bytes())
}
