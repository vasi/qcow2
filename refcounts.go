package qcow2

import (
	"encoding/binary"
	"errors"
	"log"
)

// A refcount block, or an error
type refcount struct {
	idx int64
	rc  uint64
	err error
}

// The refcounts manager of a qcow2 file
type refcounts interface {
	// Setup a new refcounts structure
	open(header)

	// Get the refcount of a block
	refcount(idx int64) (rc uint64, err error)

	// Increment a block's refcount. Must be already allocated!
	increment(idx int64) (rc uint64, err error)
	// Decrement a block's refcount.
	decrement(idx int64) (rc uint64, err error)

	// What's the maximum block index without growing the refcount table?
	max() int64

	// Allocate n new blocks, return the index of the first block
	allocate(n int64) (idx int64, err error)

	// Iterate over used blocks
	used() chan refcount
}

// Mask for valid bits of a refcount table entry
const tableValid uint64 = ^uint64(0x1ff)

// Implementation of refcounts interface
type refcountsImpl struct {
	// The qcow2 header.
	header header

	// A channel that receives free clusters
	freeClusters chan refcount
}

func (r *refcountsImpl) open(header header) {
	r.header = header
	r.freeClusters = make(chan refcount)
	go r.findFreeClusters(r.freeClusters)
}

// Get our ioAt
func (r *refcountsImpl) io() *ioAt {
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
func (r *refcountsImpl) read(block int64, count int) (rc uint64, err error) {
	offBits, fileOff, buf := r.ioInfo(block, count)
	if _, err = r.io().ReadAt(buf, fileOff); err != nil {
		return
	}

	return r.readBuf(buf, offBits), nil
}

// Write a single refcount.
// 	block - The file offset of the refcount block to write to
//  count - The index of the refcount within that block
func (r *refcountsImpl) write(block int64, count int, rc uint64) error {
	offBits, fileOff, buf := r.ioInfo(block, count)

	if r.bits() < 8 {
		// Fetch the existing content of this byte
		if _, err := r.io().ReadAt(buf, fileOff); err != nil {
			return err
		}

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

	_, err := r.io().WriteAt(buf, fileOff)
	return err
}

// Validate a table entry
func (r *refcountsImpl) validateTableEntry(tableEntry uint64) (int64, error) {
	if tableEntry&^tableValid != 0 {
		return 0, errors.New("Bad refcount table entry")
	}
	if tableEntry%uint64(r.clusterSize()) != 0 {
		return 0, errors.New("Refcount block misaligned")
	}
	return int64(tableEntry), nil
}

// Read a single table entry, given an offset (in bytes) within the table
func (r *refcountsImpl) readTableEntry(tableOffset int64) (int64, error) {
	tableEntry, err := r.io().read64(r.header.refcountOffset() + tableOffset)
	if err != nil {
		return 0, err
	}
	return r.validateTableEntry(tableEntry)
}

// An operation on refcounts
//    rc 	  - The original refcount
//    missing - Whether the original refcount is missing
//    newRc   - The desired new refcount
type rcOp func(rc uint64, missing bool) (newRc uint64, err error)

// Perform an operation on a refcount
func (r *refcountsImpl) doOp(idx int64, op rcOp) (rc uint64, err error) {
	tableOffset := r.tableOffset(idx)
	if tableOffset > int64(r.clusterSize()*r.header.refcountClusters()) {
		return op(0, true)
	}

	tableEntry, err := r.readTableEntry(tableOffset)
	count := int(idx % r.blockEntries())
	if tableEntry == 0 {
		return op(0, true)
	}

	rc, err = r.read(tableEntry, count)

	if err != nil {
		return 0, err
	}
	if rc == 0 {
		return op(0, true)
	}
	newRc, err := op(rc, false)

	if err != nil {
		return 0, err
	}
	if newRc != rc {
		err = r.write(tableEntry, count, newRc)
	}
	return newRc, err
}

func (r *refcountsImpl) refcount(idx int64) (rc uint64, err error) {
	return r.doOp(idx, func(rc uint64, missing bool) (newRc uint64, err error) {
		if missing {
			return 0, nil
		}
		return rc, nil
	})
}

func (r *refcountsImpl) increment(idx int64) (rc uint64, err error) {
	return r.doOp(idx, func(rc uint64, missing bool) (newRc uint64, err error) {
		if missing || rc == 0 {
			return 0, errors.New("Modifying unallocated refcount")
		}
		if rc == 1<<r.bits()-1 {
			return 0, errors.New("Refcount already at maximum")
		}
		return rc + 1, nil
	})
}

func (r *refcountsImpl) decrement(idx int64) (rc uint64, err error) {
	return r.doOp(idx, func(rc uint64, missing bool) (newRc uint64, err error) {
		if missing || rc == 0 {
			return 0, errors.New("Modifying unallocated refcount")
		}
		return rc - 1, nil
	})
}

// Look for free clusters, and write them to a channel
func (r *refcountsImpl) findFreeClusters(ch chan refcount) {
	var i int64
	for i = 0; true; i++ {
		rc, err := r.refcount(i)
		if err != nil || rc == 0 {
			ch <- refcount{i, 0, err}
		}
	}
}

// Find a sequence of n free clusters.
func (r *refcountsImpl) findFreeSequence(n int64) (idx int64, err error) {
	var count, start int64
	for b := range r.freeClusters {
		if b.err != nil {
			return 0, err
		}
		if count > 0 || start+count == b.idx {
			// Continue a range
			count++
		} else {
			count = 1
			start = b.idx
		}

		if count == n {
			return start, nil
		}
	}
	return 0, errors.New("Ran out of free clusters???")
}

// Create the initial reference for a new cluster
func (r *refcountsImpl) refNewCluster(idx int64) error {
	blockOff, err := r.allocRefcountBlock(idx)
	if err != nil {
		return err
	}
	count := int(idx % r.blockEntries())
	return r.write(blockOff, count, 1)
}

// Allocate a single refcount block to reference cluster idx. Return the position of the block.
func (r *refcountsImpl) allocRefcountBlock(idx int64) (blockOff int64, err error) {
	tableOffset := r.tableOffset(idx)
	if tableOffset > int64(r.clusterSize()*r.header.refcountClusters()) {
		return 0, errors.New("TODO: Grow refcount table")
	}

	if blockOff, err = r.readTableEntry(tableOffset); blockOff != 0 || err != nil {
		return blockOff, nil
	}

	// Didn't find a refcount block, must allocate one
	blockIdx, err := r.findFreeSequence(1)
	if err != nil {
		return
	}

	// Zero the new block
	blockOff = blockIdx * int64(r.clusterSize())
	if err = r.io().fill(blockOff, r.clusterSize(), 0); err != nil {
		return
	}

	// Check if the refcount for this block is inside itself
	blockBlocksStart := idx - idx%r.blockEntries()
	if blockIdx > blockBlocksStart && blockIdx-blockBlocksStart < r.blockEntries() {
		// The block is self-describing
		count := int(blockIdx % r.blockEntries())
		err = r.write(blockOff, count, 1)
	} else {
		// Not self-describing, must recurse
		err = r.refNewCluster(blockIdx)
	}

	// Write the new entry in the table
	log.Printf("BLOCK: %d\n", blockIdx)
	return blockOff, r.io().write64(tableOffset+r.header.refcountOffset(), uint64(blockOff))
}

func (r *refcountsImpl) allocate(n int64) (idx int64, err error) {
	idx, err = r.findFreeSequence(n)
	if err != nil {
		return 0, err
	}

	for i := idx; i < idx+n; i++ {
		if err = r.refNewCluster(i); err != nil {
			return 0, err
		}
	}

	return idx, err
}

func (r *refcountsImpl) used() chan refcount {
	ch := make(chan refcount)
	go func() {
		// Cache table and blocks, to quickly get new items
		// Maybe take this out if we add a caching layer?
		table := make([]byte, r.clusterSize()*r.header.refcountClusters())
		if _, err := r.io().ReadAt(table, r.header.refcountOffset()); err != nil {
			ch <- refcount{0, 0, err}
			close(ch)
			return
		}

		block := make([]byte, r.clusterSize())
		for ti := 0; ti < len(table)/8; ti++ {
			rawEntry := binary.BigEndian.Uint64(table[8*ti:])
			tableEntry, err := r.validateTableEntry(rawEntry)
			if err != nil {
				ch <- refcount{0, 0, err}
				continue
			}
			if tableEntry == 0 {
				continue
			}

			if _, err = r.io().ReadAt(block, tableEntry); err != nil {
				ch <- refcount{0, 0, err}
				continue
			}

			for i := 0; i < int(r.blockEntries()); i++ {
				bits := i * int(r.bits())
				rc := r.readBuf(block[bits/8:], bits)
				if rc != 0 {
					ch <- refcount{int64(ti)*r.blockEntries() + int64(i), rc, nil}
				}
			}
		}
		close(ch)
	}()
	return ch
}
