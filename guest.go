package qcow2

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

// Guest allows access to the data of a qcow2 file as a guest OS sees them
type Guest interface {
	// Setup a new guest
	open(header header, refcounts refcounts, l1 int64, size int64)

	// Read and write at positions
	ReaderWriterAt
	// Get the size of this disk
	Size() int64
}

// Bits for mapEntry
const (
	noCow      uint64 = 1 << 63
	compressed uint64 = 1 << 62
	zeroBit    uint64 = 1
	l1Valid    uint64 = noCow | (1<<56-1)&^0x1ff
	l2Valid    uint64 = l1Valid | compressed | zeroBit
)

// An entry in the L1 or L2 blocks
type mapEntry uint64

func (e mapEntry) compressed() bool {
	return uint64(e)&compressed != 0
}

// Is this entry a forced-zero block?
func (e mapEntry) zero() bool { // For L2 only
	return uint64(e)&zeroBit != 0
}

// Is this entry empty?
func (e mapEntry) nil() bool {
	return e.offset() == 0
}

// Does this entry contain a valid offset?
func (e mapEntry) hasOffset() bool {
	return !e.zero() && !e.nil()
}
func (e mapEntry) offset() int64 {
	return int64(uint64(e) &^ (noCow | compressed))
}

// Does this cluster need to be copied before writing?
func (e mapEntry) cow() bool {
	return uint64(e)&noCow == 0 && !e.nil()
}

// Is it safe to alter the data pointed to by this item?
func (e mapEntry) writable() bool {
	return e.hasOffset() && !e.cow()
}

type guestImpl struct {
	header     header
	refcounts  refcounts
	l1Position int64
	size       int64

	// Synchronize metadata changes only, block changes can stomp on each other
	sync.RWMutex
}

func (g *guestImpl) open(header header, refcounts refcounts, l1 int64, size int64) {
	g.header = header
	g.refcounts = refcounts
	g.l1Position = l1
	g.size = size
}

// Get the internal IO object
func (g *guestImpl) io() *ioAt {
	return g.header.io()
}

// Get the size of each cluster
func (g *guestImpl) clusterSize() int {
	return g.header.clusterSize()
}

// How big can the L1 be, in clusters?
func (g *guestImpl) l1Clusters() int {
	clusters := divceil(g.size, int64(g.clusterSize()))
	l1Entries := divceil(clusters, g.l2Entries())
	return int(divceil(l1Entries*8, int64(g.clusterSize())))
}

// How many entries in an L2 table?
func (g *guestImpl) l2Entries() int64 {
	return int64(g.clusterSize() / 8)
}

// Validate an L1 entry
func (g *guestImpl) validateL1(e mapEntry) error {
	if e.offset()%int64(g.clusterSize()) != 0 {
		return errors.New("Misaligned mapping entry")
	}
	return nil
}

// Validate an L2 entry
func (g *guestImpl) validateL2(e mapEntry) error {
	if e.zero() {
		return nil
	}
	if e.compressed() {
		return errors.New("Compression not supported")
	}
	return g.validateL1(e)
}

// Validates an entry
type entryValidator func(mapEntry) error

// Get an L1 or L2 entry.
//
// validator - a function to make sure the entry is valid
// off		 - the offset into the file where the entry is found
// writable  - whether or not the cluster the entry points to needs to be safe for
//		       writing on return
func (g *guestImpl) getEntry(validator entryValidator, off int64, writable bool) (e mapEntry, err error) {
	var v uint64
	if v, err = g.io().read64(off); err != nil {
		return
	}
	e = mapEntry(v)
	if err = validator(e); err != nil || !writable || e.writable() {
		return
	}

	// Need to make it writable, so allocate a new block
	allocIdx, err := g.refcounts.allocate(1)
	if err != nil {
		return
	}
	old := e
	alloc := allocIdx * int64(g.clusterSize())
	e = mapEntry(uint64(alloc) | noCow)

	// Initialize the new block
	if old.hasOffset() {
		err = g.io().copy(alloc, old.offset(), g.clusterSize())
	} else {
		err = g.io().fill(alloc, g.clusterSize(), 0)
	}
	if err != nil {
		return
	}

	// Write it to the parent
	if err = g.io().write64(off, uint64(e)); err != nil {
		return
	}

	// Deref the old value
	if old.hasOffset() {
		if _, err = g.refcounts.decrement(old.offset() / int64(g.clusterSize())); err != nil {
			return
		}
	}

	return e, nil
}

// Get the L1 entry for the cluster at the given guest index
func (g *guestImpl) getL1(idx int64, writable bool) (mapEntry, error) {
	off := g.l1Position + (idx/g.l2Entries())*8
	return g.getEntry(g.validateL1, off, writable)
}

// Get the L2 entry for the cluster at the given guest index
func (g *guestImpl) getL2(idx int64, writable bool) (l1 mapEntry, err error) {
	l1, err = g.getL1(idx, writable)
	if err != nil || (!writable && l1.nil()) {
		return
	}
	off := l1.offset() + (idx%g.l2Entries())*8
	return g.getEntry(g.validateL2, off, writable)
}

// Fill a slice with zeros
func zeroFill(p []byte) {
	for i := 0; i < len(p); i++ {
		p[i] = 0
	}
}

// Read a segment of a cluster, given its L2 entry
func (g *guestImpl) readByL2(p []byte, l2 mapEntry, off int) error {
	if l2.nil() || l2.zero() {
		zeroFill(p)
	} else {
		if _, err := g.io().ReadAt(p, l2.offset()+int64(off)); err != nil {
			return err
		}
	}
	return nil
}

// Read a segment of a cluster
// p   - The buffer to read into
// idx - The index of the cluster within the guest disk
// off - The offset inside the cluster to start reading
func (g *guestImpl) readCluster(p []byte, idx int64, off int) (err error) {
	var l2 mapEntry
	func() {
		g.RLock()
		defer g.RUnlock()
		l2, err = g.getL2(idx, false)
	}()
	if err != nil {
		return err
	}
	return g.readByL2(p, l2, off)
}

// Write a segment of a cluster
func (g *guestImpl) writeCluster(p []byte, idx int64, off int) (err error) {
	// Check if there are any changes
	orig := make([]byte, len(p))
	if err = g.readCluster(orig, idx, off); err != nil {
		return
	}
	if bytes.Compare(orig, p) == 0 {
		// No changes, don't do anything
		return nil
	}

	var l2 mapEntry
	func() {
		g.Lock()
		defer g.Unlock()

		// Must autoclear header before first write
		if err = g.header.autoclear(); err != nil {
			return
		}

		// Get a writable L2 entry
		l2, err = g.getL2(idx, true)
	}()
	if err != nil {
		return
	}

	_, err = g.io().WriteAt(p, l2.offset()+int64(off))
	return
}

// A function to process a cluster
type clusterFunc func(g *guestImpl, p []byte, idx int64, off int) error

// Given a slice that may span clusters, break it down into single-cluster operations
func (g *guestImpl) perCluster(p []byte, off int64, f clusterFunc) (n int, err error) {
	if off+int64(len(p)) > g.size {
		return 0, io.ErrUnexpectedEOF
	}

	idx := int64(off / int64(g.clusterSize()))
	offset := int(off % int64(g.clusterSize()))
	n = 0
	for len(p) > 0 {
		length := g.clusterSize() - offset
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
