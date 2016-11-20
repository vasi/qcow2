package qcow2

import "errors"

type refcounts interface {
	open(header)

	refcount(idx int64) (rc int64, err error)
	increment(idx int64) (rc int64, err error)
	decrement(idx int64) (rc int64, err error)
	max() int64

	allocate(n int64) (idx int64, err error)
}

const tableValid uint64 = ^uint64(0x1ff)

type refcountsImpl struct {
	header header
}

func (r *refcountsImpl) open(header header) {
	r.header = header
}

func (r *refcountsImpl) io() *ioAt {
	return r.header.io()
}

func (r *refcountsImpl) bits() uint {
	return uint(r.header.refcountBits())
}

func (r *refcountsImpl) clusterSize() int {
	return r.header.clusterSize()
}

func (r *refcountsImpl) blockEntries() int64 {
	return int64(r.clusterSize() * 8 / int(r.bits()))
}

func (r *refcountsImpl) max() int64 {
	return int64(r.header.refcountClusters()) * int64(r.clusterSize()/8) * r.blockEntries()
}

func (r *refcountsImpl) read(block uint64, count int) (rc int64, err error) {
	offBits := int(r.bits()) * count
	off := int64(block + uint64(offBits/8))
	bytes := divceil(int64(r.bits()), 8)
	buf := make([]byte, bytes)
	if _, err = r.io().ReadAt(buf, off); err != nil {
		return
	}

	rc = 0
	for i := 0; i < len(buf); i++ {
		rc <<= 8
		rc += int64(buf[i])
	}

	rc >>= uint(offBits % 8)
	return
}

func (r *refcountsImpl) write(block uint64, count int, rc int64) error {
	offBits := int(r.bits()) * count
	off := int64(block + uint64(offBits/8))
	bytes := divceil(int64(count), 8)
	buf := make([]byte, bytes)

	if r.bits() < 8 {
		// Fetch the existing content of this byte
		if _, err := r.io().ReadAt(buf, off); err != nil {
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

	_, err := r.io().WriteAt(buf, off)
	return err
}

func (r *refcountsImpl) readTableEntry(tableOffset int64) (uint64, error) {
	tableEntry, err := r.io().read64(tableOffset)
	if err != nil {
		return 0, err
	}
	if tableEntry&^tableValid != 0 {
		return 0, errors.New("Bad refcount table entry")
	}
	if tableEntry%uint64(r.clusterSize()) != 0 {
		return 0, errors.New("Refcount block misaligned")
	}
	return tableEntry, nil
}

type rcOp func(rc int64, missing bool) (newRc int64, err error)

func (r *refcountsImpl) doOp(idx int64, op rcOp) (rc int64, err error) {
	tableOffset := r.header.refcountOffset() + 8*idx/r.blockEntries()
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

func (r *refcountsImpl) refcount(idx int64) (rc int64, err error) {
	return r.doOp(idx, func(rc int64, missing bool) (newRc int64, err error) {
		if missing {
			return 0, nil
		}
		return rc, nil
	})
}

func (r *refcountsImpl) increment(idx int64) (rc int64, err error) {
	return r.doOp(idx, func(rc int64, missing bool) (newRc int64, err error) {
		if missing || rc == 0 {
			return 0, errors.New("Modifying unallocated refcount")
		}
		if rc == 1<<r.bits()-1 {
			return 0, errors.New("Refcount already at maximum")
		}
		return rc + 1, nil
	})
}

func (r *refcountsImpl) decrement(idx int64) (rc int64, err error) {
	return r.doOp(idx, func(rc int64, missing bool) (newRc int64, err error) {
		if missing || rc == 0 {
			return 0, errors.New("Modifying unallocated refcount")
		}
		return rc - 1, nil
	})
}

func (r *refcountsImpl) allocate(n int64) (idx int64, err error) {
	// TODO
	return 0, nil
}
