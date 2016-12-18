package qcow2

import (
	"github.com/timtadh/data-structures/exc"
	"github.com/vasi/go-qcow2/eio"
)

// A refcount status
type refcount struct {
	idx int64
	rc  uint64
}

// The refcounts manager of a qcow2 file
type refcounts interface {
	// Setup a new refcounts structure
	open(header)
	close()

	// Get the refcount of a block
	refcount(idx int64) uint64

	// Increment a block's refcount. Must be already allocated!
	increment(idx int64) uint64
	// Decrement a block's refcount.
	decrement(idx int64) uint64

	// What's the maximum block index without growing the refcount table?
	max() int64

	// Allocate n new blocks, return the index of the first block
	allocate(n int64) int64

	// Iterate over used blocks
	used(*eio.Pipeline) <-chan refcount
}

// Mask for valid bits of a refcount table entry
const tableValid uint64 = ^uint64(0x1ff)

// Implementation of refcounts interface
type refcountsImpl struct {
	// The qcow2 header.
	header header

	// A pipeline for free clusters
	freeClustersPipeline *eio.Pipeline
	// A channel that receives free clusters
	freeClusters <-chan int64
}

func (r *refcountsImpl) open(header header) {
	r.header = header

	// Setup free cluster finding
	r.freeClustersPipeline = eio.NewPipeline()
	r.freeClusters = r.findFreeClusters(r.freeClustersPipeline)
}

func (r *refcountsImpl) close() {
	r.freeClustersPipeline.WaitThrow()
}

// Get our IO
func (r *refcountsImpl) io() *eio.BinaryIO {
	return r.header.io()
}

// How many bits in a refcount?
func (r *refcountsImpl) bits() uint {
	return uint(r.header.refcountBits())
}

// How many bytes in a cluster?
func (r *refcountsImpl) clusterSize() int {
	return r.header.clusterSize()
}

// How many entries can go in one refcount block?
func (r *refcountsImpl) blockEntries() int64 {
	return int64(r.clusterSize() * 8 / int(r.bits()))
}

// The offset within refcount table for a block
func (r *refcountsImpl) tableOffset(idx int64) int64 {
	blockIdx := idx / r.blockEntries()
	return 8 * blockIdx
}

func (r *refcountsImpl) max() int64 {
	return int64(r.header.refcountClusters()) * int64(r.clusterSize()/8) * r.blockEntries()
}

// Get some useful info for refcount reading and writing.
func (r *refcountsImpl) ioInfo(block int64, count int) (offBits int, fileOff int64, buf []byte) {
	offBits = int(r.bits()) * count
	fileOff = block + int64(offBits/8)
	bufSize := divceil(int64(r.bits()), 8)
	buf = make([]byte, bufSize)
	return
}

// Read a refcount in a buffer, by count
func (r *refcountsImpl) readBuf(buf []byte, bits int) (rc uint64) {
	nbytes := divceil(int64(r.bits()), 8)
	rc = 0
	for i := 0; i < int(nbytes); i++ {
		rc <<= 8
		rc += uint64(buf[i])
	}

	rc >>= uint(bits % 8)
	rc &= (uint64(1) << r.bits()) - 1
	return rc
}

// Read a single refcount.
// 	block - The file offset of the refcount block to read from
//  count - The index of the refcount within that block
func (r *refcountsImpl) read(block int64, count int) uint64 {
	offBits, fileOff, buf := r.ioInfo(block, count)
	r.io().ReadAt(fileOff, buf)
	return r.readBuf(buf, offBits)
}

// Write a single refcount.
// 	block - The file offset of the refcount block to write to
//  count - The index of the refcount within that block
func (r *refcountsImpl) write(block int64, count int, rc uint64) {
	offBits, fileOff, buf := r.ioInfo(block, count)

	if r.bits() < 8 {
		// Fetch the existing content of this byte
		r.io().ReadAt(fileOff, buf)

		shift := uint(offBits % 8)
		mask := byte((1 << r.bits()) - 1)
		// Mask out the old value
		buf[0] &^= mask << shift
		// Or in the new value
		buf[0] |= (byte(rc) & mask) << shift
	} else {
		for i := 0; i < len(buf); i++ {
			buf[len(buf)-i-1] = byte(rc & 0xff)
			rc >>= 8
		}
	}

	r.io().WriteAt(fileOff, buf)
}

// Validate a table entry
func (r *refcountsImpl) validateTableEntry(tableEntry uint64) int64 {
	if tableEntry&^tableValid != 0 {
		exc.Throwf("Bad refcount table entry")
	}
	if tableEntry%uint64(r.clusterSize()) != 0 {
		exc.Throwf("Refcount block misaligned")
	}
	return int64(tableEntry)
}

// Read a single table entry, given an offset (in bytes) within the table
func (r *refcountsImpl) readTableEntry(tableOffset int64) int64 {
	entry := r.io().ReadUint64(r.header.refcountOffset() + tableOffset)
	return r.validateTableEntry(entry)
}

// An operation on refcounts
//    rc 	  - The original refcount
//    missing - Whether the original refcount is missing
//    Returns the desired new refcount
type rcOp func(rc uint64, missing bool) uint64

// Perform an operation on a refcount
func (r *refcountsImpl) refcountOp(idx int64, op rcOp) uint64 {
	tableOffset := r.tableOffset(idx)
	if tableOffset > int64(r.clusterSize()*r.header.refcountClusters()) {
		return op(0, true)
	}

	tableEntry := r.readTableEntry(tableOffset)
	if tableEntry == 0 {
		return op(0, true)
	}

	count := int(idx % r.blockEntries())
	rc := r.read(tableEntry, count)
	if rc == 0 {
		return op(0, true)
	}

	newRc := op(rc, false)
	if newRc != rc {
		r.write(tableEntry, count, newRc)
	}
	return newRc
}

func (r *refcountsImpl) refcount(idx int64) uint64 {
	return r.refcountOp(idx, func(rc uint64, missing bool) uint64 {
		if missing {
			return 0
		}
		return rc
	})
}

func (r *refcountsImpl) increment(idx int64) uint64 {
	return r.refcountOp(idx, func(rc uint64, missing bool) uint64 {
		if missing || rc == 0 {
			exc.Throwf("Modifying unallocated refcount")
		}
		if rc == 1<<r.bits()-1 {
			exc.Throwf("Refcount already at maximum")
		}
		return rc + 1
	})
}

func (r *refcountsImpl) decrement(idx int64) uint64 {
	return r.refcountOp(idx, func(rc uint64, missing bool) uint64 {
		if missing || rc == 0 {
			exc.Throwf("Modifying unallocated refcount")
		}
		return rc - 1
	})
}

// Look for free clusters, and write them to a channel
func (r *refcountsImpl) findFreeClusters(pipe *eio.Pipeline) <-chan int64 {
	ch := make(chan int64)
	pipe.Go(func() {
		defer close(ch)

		// Quickly find a place to start
		var rc refcount
		for rc = range r.fastScan(pipe, false) {
			if rc.rc == 0 {
				break
			}
		}

		var i int64
		for i = rc.idx; true; i++ {
			if r.refcount(i) == 0 {
				select {
				case <-pipe.Done():
					return
				case ch <- i:
				}
			}
		}
	})
	return ch
}

// Find a sequence of n free clusters.
func (r *refcountsImpl) findFreeSequence(n int64) int64 {
	var count, start int64
	for b := range r.freeClusters {
		if count > 0 || start+count == b {
			// Continue a range
			count++
		} else {
			count = 1
			start = b
		}

		if count == n {
			return start
		}
	}
	r.freeClustersPipeline.WaitThrow()
	return 0 // Must have been an error, will throw above
}

// Create the initial reference for a new cluster
func (r *refcountsImpl) refNewCluster(idx int64) {
	blockOff := r.allocRefcountBlock(idx)
	count := int(idx % r.blockEntries())
	r.write(blockOff, count, 1)
}

// Grow the top-level refcount table.
func (r *refcountsImpl) growTable() {
	newTableStart := r.findFreeSequence(1)

	// Find an appropriate table size
	var newBlocks int64
	preTableClusters := newTableStart % r.blockEntries()
	tableSize := int64(r.header.refcountClusters())
	for {
		// Try a new size
		tableSize *= 2

		// Solve: preTableClusters + tableSize + n <= n * r.blockEntries()
		//     -> n >= (preTableClusters + tableSize) / (r.blockEntries() - 1)
		newBlocks = (preTableClusters+tableSize-1)/(r.blockEntries()-1) + 1

		// Make sure it fits in the new table
		maxOffset := tableSize * int64(r.clusterSize())
		if r.tableOffset(newTableStart+tableSize+newBlocks) <= maxOffset {
			break
		}
	}

	// Create and fill the new table
	next := r.findFreeSequence(tableSize - 1 + newBlocks)
	if next != newTableStart+1 {
		exc.Throwf("Couldn't allocate refcount table???")
	}
	cs := r.clusterSize()
	r.io().Copy(newTableStart*int64(cs), r.header.refcountOffset(),
		r.header.refcountClusters()*cs)
	diff := int(tableSize) - r.header.refcountClusters()
	r.io().Zero((newTableStart+int64(r.header.refcountClusters()))*int64(cs), diff*cs)

	// Create the new blocks, and put them in the table
	blockBase := newTableStart + tableSize
	blockOff := r.tableOffset(newTableStart)
	newTablePos := newTableStart * int64(cs)
	for b := 0; int64(b) < newBlocks; b++ {
		blockPos := (blockBase + int64(b)) * int64(cs)
		r.io().Zero(blockPos, cs)
		r.io().WriteUint64(newTablePos+blockOff, uint64(blockPos))
		blockOff += 8
	}

	// Set refcounts in the new blocks
	newClusterOffset := newTableStart % int64(cs)
	for b := 0; int64(b) < tableSize+newBlocks; b++ {
		newBlock := blockBase + newClusterOffset/r.blockEntries()
		r.write(newBlock*int64(cs), int(newClusterOffset%r.blockEntries()), 1)
		newClusterOffset++
	}

	// Set the header values
	oldSize := r.header.refcountClusters()
	oldIdx := r.header.refcountOffset() / int64(r.clusterSize())
	r.header.setRefcountTable(newTableStart*int64(cs), int(tableSize))

	// Deref the old table
	for i := 0; i < oldSize; i++ {
		r.decrement(oldIdx + int64(i))
	}
}

// Allocate a single refcount block to reference cluster idx. Return the position of the block.
func (r *refcountsImpl) allocRefcountBlock(idx int64) int64 {
	tableOffset := r.tableOffset(idx)
	if tableOffset >= int64(r.clusterSize()*r.header.refcountClusters()) {
		r.growTable()
	}

	if blockOff := r.readTableEntry(tableOffset); blockOff != 0 {
		return blockOff
	}

	// Didn't find a refcount block, must allocate one
	blockIdx := r.findFreeSequence(1)
	// Zero the new block
	blockOff := blockIdx * int64(r.clusterSize())
	r.io().Zero(blockOff, r.clusterSize())

	// Check if the refcount for this block is inside itself
	blockBlocksStart := idx - idx%r.blockEntries()
	if blockIdx > blockBlocksStart && blockIdx-blockBlocksStart < r.blockEntries() {
		// The block is self-describing
		count := int(blockIdx % r.blockEntries())
		r.write(blockOff, count, 1)
	} else {
		// Not self-describing, must recurse
		r.refNewCluster(blockIdx)
	}

	// Write the new entry in the table
	r.io().WriteUint64(tableOffset+r.header.refcountOffset(), uint64(blockOff))
	return blockOff
}

func (r *refcountsImpl) allocate(n int64) int64 {
	idx := r.findFreeSequence(n)
	for i := idx; i < idx+n; i++ {
		r.refNewCluster(i)
	}
	return idx
}

func (r *refcountsImpl) fastScan(pipe *eio.Pipeline, onlyUsed bool) <-chan refcount {
	ch := make(chan refcount)
	pipe.Go(func() {
		defer close(ch)

		// Cache table and blocks, to quickly get new items
		// Maybe take this out if we add a caching layer?
		table := make([]byte, r.clusterSize()*r.header.refcountClusters())
		r.io().ReadAt(r.header.refcountOffset(), table)

		block := make([]byte, r.clusterSize())
		for ti := 0; ti < len(table)/8; ti++ {
			rawEntry := r.io().ByteOrder().Uint64(table[8*ti:])
			tableEntry := r.validateTableEntry(rawEntry)
			if tableEntry == 0 && onlyUsed {
				continue
			}
			if tableEntry != 0 { // Read the table
				r.io().ReadAt(tableEntry, block)
			}

			var rc uint64
			for i := 0; i < int(r.blockEntries()); i++ {
				cl := int64(ti)*r.blockEntries() + int64(i)
				if tableEntry == 0 {
					rc = 0
				} else {
					bits := i * int(r.bits())
					rc = r.readBuf(block[bits/8:], bits)
				}
				select {
				case <-pipe.Done():
					return
				case ch <- refcount{cl, rc}:
				}
			}
		}
	})
	return ch
}

func (r *refcountsImpl) used(pipe *eio.Pipeline) <-chan refcount {
	return r.fastScan(pipe, true)
}
