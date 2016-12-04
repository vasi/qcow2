package qcow2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"
)

// A Snapshot represents a snapshot of a qcow2 state
type Snapshot interface {
	Guest() Guest
	VMState() Guest

	GuestSize() int64
	VMStateSize() int64

	ID() string
	Name() string

	Creation() time.Time
	GuestUptime() int64 // in nsec
}

type snapshotHeader struct {
	L1TableOffset   uint64
	L1Size          uint32
	IDSize          uint16
	NameSize        uint16
	CreationSeconds uint32
	CreationNsec    uint32
	Uptime          uint64
	VMStateSize     uint32
	ExtraSize       uint32
}

type snapshotImpl struct {
	header header

	l1Position   int64
	l1Entries    int
	id           string
	name         string
	creation     time.Time
	uptime       int64
	vmStateSize  int64
	guestSize    int64
	unknownExtra []byte
}

func (s *snapshotImpl) Guest() Guest {
	// TODO
	return nil
}

func (s *snapshotImpl) VMState() Guest {
	// TODO
	return nil
}

func (s *snapshotImpl) GuestSize() int64 {
	return s.guestSize
}

func (s *snapshotImpl) VMStateSize() int64 {
	return s.vmStateSize
}

func (s *snapshotImpl) ID() string {
	return s.id
}

func (s *snapshotImpl) Name() string {
	return s.name
}

func (s *snapshotImpl) Creation() time.Time {
	return s.creation
}

func (s *snapshotImpl) GuestUptime() int64 {
	return s.uptime
}

func readSnapshots(h header) (snaps []Snapshot, err error) {
	snaps = make([]Snapshot, 0)
	if h.snapshotsOffset() == 0 {
		return
	}

	var snap *snapshotImpl
	off := h.snapshotsOffset()
	r := io.NewSectionReader(h.io(), off, math.MaxInt64-off)
	for i := 0; i < int(h.snapshotsCount()); i++ {
		if snap, err = readSnapshot(h, r); err != nil {
			return
		}
		snaps = append(snaps, snap)
	}
	return
}

func readSnapshot(h header, r *io.SectionReader) (snap *snapshotImpl, err error) {
	var sh snapshotHeader
	if err = binary.Read(r, binary.BigEndian, &sh); err != nil {
		return
	}
	if h.version() >= 3 && sh.ExtraSize < 16 {
		return nil, errors.New("Too short snapshot data for version 3")
	}

	snap = &snapshotImpl{
		header:      h,
		l1Position:  int64(sh.L1TableOffset),
		l1Entries:   int(sh.L1Size),
		creation:    time.Unix(int64(sh.CreationSeconds), int64(sh.CreationNsec)),
		uptime:      int64(sh.Uptime),
		vmStateSize: int64(sh.VMStateSize),
		guestSize:   h.size(),
	}

	buf := make([]byte, sh.ExtraSize)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	extra := bytes.NewBuffer(buf)
	if err = snap.readExtra(extra); err != nil {
		return
	}
	snap.unknownExtra = extra.Bytes()

	buf = make([]byte, sh.IDSize)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	snap.id = string(buf)
	buf = make([]byte, sh.NameSize)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	snap.name = string(buf)

	// 8-byte alignment
	cur, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}
	if cur%8 != 0 {
		if _, err = r.Seek(8-cur%8, io.SeekCurrent); err != nil {
			return
		}
	}

	return
}

func (s *snapshotImpl) readExtra(b *bytes.Buffer) (err error) {
	var v uint64

	if b.Len() < 8 {
		return
	}
	if err = binary.Read(b, binary.BigEndian, &v); err != nil {
		return
	}
	s.vmStateSize = int64(v)

	if b.Len() < 8 {
		return
	}
	if err = binary.Read(b, binary.BigEndian, &v); err != nil {
		return
	}
	s.guestSize = int64(v)

	return nil
}
