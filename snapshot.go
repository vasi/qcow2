package qcow2

import (
	"io"
	"math"
	"time"

	"github.com/timtadh/data-structures/exc"
	"github.com/vasi/go-qcow2/eio"
)

// A Snapshot represents a snapshot of a qcow2 state
type Snapshot interface {
	Guest() (Guest, error)
	VMState() (Guest, error)

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

func (s *snapshotImpl) Guest() (Guest, error) {
	// TODO
	return nil, nil
}

func (s *snapshotImpl) VMState() (Guest, error) {
	// TODO
	return nil, nil
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

func readSnapshots(h header) []Snapshot {
	snaps := make([]Snapshot, 0)
	if h.snapshotsOffset() == 0 {
		return snaps
	}

	off := h.snapshotsOffset()
	r := bio.NewReaderSection(h.io(), off, math.MaxInt64-off)
	for i := 0; i < int(h.snapshotsCount()); i++ {
		snaps = append(snaps, readSnapshot(h, r))
	}
	return snaps
}

func readSnapshot(h header, r *bio.SequentialReader) *snapshotImpl {
	var sh snapshotHeader
	r.ReadData(&sh)
	if h.version() >= 3 && sh.ExtraSize < 16 {
		exc.Throwf("Too short snapshot data for version 3")
	}

	snap := &snapshotImpl{
		header:      h,
		l1Position:  int64(sh.L1TableOffset),
		l1Entries:   int(sh.L1Size),
		creation:    time.Unix(int64(sh.CreationSeconds), int64(sh.CreationNsec)),
		uptime:      int64(sh.Uptime),
		vmStateSize: int64(sh.VMStateSize),
		guestSize:   h.size(),
	}

	snap.readExtra(r.SubReader(int64(sh.ExtraSize)))
	snap.id = string(r.ReadNewBuf(int(sh.IDSize)))
	snap.name = string(r.ReadNewBuf(int(sh.NameSize)))
	r.Align(8)
	return snap
}

func (s *snapshotImpl) readExtra(r *bio.SequentialReader) {
	exc.Try(func() {
		s.vmStateSize = int64(r.ReadUint64())
		s.guestSize = int64(r.ReadUint64())
	}).Catch(&exc.Exception{}, func(t exc.Throwable) {
		if t.Exc().Errors[0].Err == io.EOF {
			exc.Rethrow(t, exc.Errorf("Reading snapshot extra data"))
		}
	}).Unwind()

	rem := r.Remain()
	if rem > 0 {
		s.unknownExtra = r.ReadNewBuf(int(rem))
	}
}
