package qcow2

import (
	"bytes"
	"io"
	"sync"

	"github.com/timtadh/data-structures/exc"
	"github.com/vasi/go-qcow2/eio"
)

// Guest allows access to the data of a qcow2 file as a guest OS sees them
type Guest interface {
	// Setup a new guest
	open(header header, refcounts refcounts, l1 int64, size int64)
	Close() error

	// Read and write at positions
	eio.ReaderWriterAt
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

func (g *guestImpl) Close() error {
	return eio.BacktraceWrap(func() {
		g.header.close()
		g.refcounts.close()
	})
}

func (g *guestImpl) io() *eio.BinaryIO {
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
func (g *guestImpl) validateL1(e mapEntry) {
	if e.offset()%int64(g.clusterSize()) != 0 {
		exc.Throwf("Misaligned mapping entry")
	}
}

// Validate an L2 entry
func (g *guestImpl) validateL2(e mapEntry) {
	if e.zero() {
		return
	}
	if e.compressed() {
		exc.Throwf("Compression not supported")
	}
	g.validateL1(e)
}

// Validates an entry
type entryValidator func(mapEntry)

// Get an L1 or L2 entry.
//
// validator - a function to make sure the entry is valid
// off		 - the offset into the file where the entry is found
// writable  - whether or not the cluster the entry points to needs to be safe for
//		       writing on return
func (g *guestImpl) getEntry(validator entryValidator, off int64, writable bool) mapEntry {
	oldEntry := mapEntry(g.io().ReadUint64(off))
	validator(oldEntry)
	if !writable || oldEntry.writable() {
		return oldEntry
	}

	// Need to make it writable, so allocate a new block
	alloc := g.refcounts.allocate(1) * int64(g.clusterSize())
	newEntry := mapEntry(uint64(alloc) | noCow)

	// Initialize the new block
	if oldEntry.hasOffset() {
		g.io().Copy(alloc, oldEntry.offset(), g.clusterSize())
	} else {
		g.io().Zero(alloc, g.clusterSize())
	}

	// Write it to the parent
	g.io().WriteUint64(off, uint64(newEntry))

	// Deref the old value
	if oldEntry.hasOffset() {
		g.refcounts.decrement(oldEntry.offset() / int64(g.clusterSize()))
	}

	return newEntry
}

// Get the L1 entry for the cluster at the given guest index
func (g *guestImpl) getL1(idx int64, writable bool) mapEntry {
	off := g.l1Position + (idx/g.l2Entries())*8
	return g.getEntry(g.validateL1, off, writable)
}

// Get the L2 entry for the cluster at the given guest index
func (g *guestImpl) getL2(idx int64, writable bool) mapEntry {
	l1 := g.getL1(idx, writable)
	if !writable && l1.nil() {
		return l1
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
func (g *guestImpl) readByL2(p []byte, l2 mapEntry, off int) {
	if l2.nil() || l2.zero() {
		zeroFill(p)
	} else {
		g.io().ReadAt(l2.offset()+int64(off), p)
	}
}

// Read a segment of a cluster
// p   - The buffer to read into
// idx - The index of the cluster within the guest disk
// off - The offset inside the cluster to start reading
func (g *guestImpl) readCluster(p []byte, idx int64, off int) {
	var l2 mapEntry
	func() {
		g.RLock()
		defer g.RUnlock()
		l2 = g.getL2(idx, false)
	}()
	g.readByL2(p, l2, off)
}

// Write a segment of a cluster
func (g *guestImpl) writeCluster(p []byte, idx int64, off int) {
	// Check if there are any changes
	orig := make([]byte, len(p))
	g.readCluster(orig, idx, off)
	if bytes.Compare(orig, p) == 0 {
		// No changes, don't do anything
		return
	}

	var l2 mapEntry
	func() {
		g.Lock()
		defer g.Unlock()

		// Must autoclear header before first write
		g.header.autoclear()

		// Get a writable L2 entry
		l2 = g.getL2(idx, true)
	}()

	g.io().WriteAt(l2.offset()+int64(off), p)
	return
}

// A function to process a cluster
type clusterFunc func(g *guestImpl, p []byte, idx int64, off int)

// Given a slice that may span clusters, break it down into single-cluster operations
func (g *guestImpl) perCluster(p []byte, off int64, f clusterFunc) int {
	if off+int64(len(p)) > g.size {
		exc.ThrowOnError(io.ErrUnexpectedEOF)
	}

	idx := int64(off / int64(g.clusterSize()))
	offset := int(off % int64(g.clusterSize()))
	bytes := 0
	for len(p) > 0 {
		length := g.clusterSize() - offset
		if length > len(p) {
			length = len(p)
		}
		f(g, p[:length], idx, offset)
		p = p[length:]
		idx++
		offset = 0
		bytes += length
	}
	return bytes
}

func (g *guestImpl) ReadAt(p []byte, off int64) (n int, err error) {
	err = eio.BacktraceWrap(func() {
		n = g.perCluster(p, off, (*guestImpl).readCluster)
	})
	return
}

func (g *guestImpl) WriteAt(p []byte, off int64) (n int, err error) {
	err = eio.BacktraceWrap(func() {
		n = g.perCluster(p, off, (*guestImpl).writeCluster)
	})
	return
}

func (g *guestImpl) Size() int64 {
	return g.size
}
