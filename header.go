package qcow2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type header interface {
	open(ReaderWriterAt) error
	close() error

	write() error

	clusterSize() int

	l1Size() int
	l1Offset() int64
	size() int64

	io() ioAt
}

const (
	magic                uint32 = 0x514649fb
	featureDirty         uint64 = 1
	featureCorrupt       uint64 = 2
	incompatibleKnown    uint64 = featureDirty | featureCorrupt
	featureLazyRefcounts uint64 = 1
	featureBitmaps       uint64 = 1
	autoclearKnown       uint64 = featureBitmaps
	v2Length             uint32 = 72
)

type headerV2 struct {
	Magic                 uint32
	Version               uint32
	BackingFileOffset     uint64
	BackingFileSize       uint32
	ClusterBits           uint32
	Size                  uint64
	CryptMethod           uint32
	L1Size                uint32
	L1TableOffset         uint64
	RefcountTableOffset   uint64
	RefcountTableClusters uint32
	NbSnapshots           uint32
	SnapshotsOffset       uint64
}

type headerV3 struct {
	IncompatibleFeatures uint64
	CompatibleFeatures   uint64
	AutoclearFeatures    uint64
	RefcountOrder        uint32
	HeaderLength         uint32
}

type headerImpl struct {
	ioAt        ioAt
	v2          headerV2
	v3          headerV3
	extraHeader []byte
	extensions  map[uint32][]byte
}

func (h *headerImpl) open(rw ReaderWriterAt) error {
	h.ioAt = ioAt{rw, binary.BigEndian}

	// Validate some basic fields.
	if err := h.ioAt.readAt(0, &h.v2.Magic); err != nil {
		return err
	}
	if h.v2.Magic != magic {
		return errors.New("Not a qcow2 file")
	}

	if err := h.ioAt.readAt(4, &h.v2.Version); err != nil {
		return err
	}
	if h.v2.Version < 2 || h.v2.Version > 3 {
		return fmt.Errorf("Unsupported qcow2 format version %d", h.v2.Version)
	}

	if err := h.ioAt.readAt(20, &h.v2.ClusterBits); err != nil {
		return err
	}
	if h.v2.ClusterBits < 9 || h.v2.ClusterBits > 21 {
		return fmt.Errorf("Invalid qcow2 cluster bits %d", h.v2.ClusterBits)
	}

	// Make sure we don't read too far.
	section := io.NewSectionReader(rw, 0, 1<<h.v2.ClusterBits)
	return h.read(section)
}

func (h *headerImpl) read(r *io.SectionReader) error {
	// Read v2 header
	if err := binary.Read(r, binary.BigEndian, &h.v2); err != nil {
		return err
	}

	// Validate fields
	if h.v2.BackingFileOffset != 0 {
		return errors.New("Backing files are not supported")
	}
	if h.v2.CryptMethod != 0 {
		return errors.New("Encryption is not supported")
	}

	guestBlocks := divceil(int64(h.v2.Size), int64(h.clusterSize()))
	l2Entries := h.clusterSize() / 8
	l1Entries := divceil(guestBlocks, int64(l2Entries))
	if l1Entries > int64(h.v2.L1Size) {
		return errors.New("Too few L1 entries for disk size")
	}

	if h.v2.L1TableOffset == 0 {
		return errors.New("Missing L1 table")
	}
	if h.v2.L1TableOffset%uint64(h.clusterSize()) != 0 {
		return errors.New("Unaligned L1 table")
	}
	if h.v2.RefcountTableOffset%uint64(h.clusterSize()) != 0 {
		return errors.New("Unaligned refcount table")
	}
	if h.v2.RefcountTableClusters == 0 || h.v2.RefcountTableOffset == 0 {
		return errors.New("Missing refcount table")
	}
	if h.v2.SnapshotsOffset%uint64(h.clusterSize()) != 0 {
		return errors.New("Unaligned snapshots")
	}

	if h.v2.Version == 2 {
		h.v3.IncompatibleFeatures = 0
		h.v3.CompatibleFeatures = 0
		h.v3.AutoclearFeatures = 0
		h.v3.RefcountOrder = 4
		h.v3.HeaderLength = v2Length
	} else {
		// Read v3 header
		if err := binary.Read(r, binary.BigEndian, &h.v3); err != nil {
			return err
		}

		if unknown := h.v3.IncompatibleFeatures &^ incompatibleKnown; unknown != 0 {
			return fmt.Errorf("Unknown incompatible features %0x", unknown)
		}
		if h.v3.IncompatibleFeatures&featureCorrupt != 0 {
			return errors.New("Corrupt bit is set")
		}
		if h.v3.RefcountOrder == 0 || h.v3.RefcountOrder > 6 {
			return fmt.Errorf("Bad refcount order %d", h.v3.RefcountOrder)
		}
	}

	// Read any extra header bytes.
	pos, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	diff := int64(h.v3.HeaderLength) - pos
	if diff < 0 {
		return fmt.Errorf("Header data is longer than claimed length %d", h.v3.HeaderLength)
	} else if diff > 0 {
		h.extraHeader = make([]byte, diff)
		if _, err := io.ReadFull(r, h.extraHeader); err != nil {
			return err
		}
	}

	return h.readExtensions(r)
}

func (h *headerImpl) readExtensions(r *io.SectionReader) error {
	var extensionId, extensionSize uint32
	h.extensions = make(map[uint32][]byte)
	for {
		if err := binary.Read(r, binary.BigEndian, &extensionId); err != nil {
			return err
		}
		if extensionId == 0 {
			break
		}

		if err := binary.Read(r, binary.BigEndian, &extensionSize); err != nil {
			return err
		}
		if int64(extensionSize) > r.Size() {
			return fmt.Errorf("Extension too long, %d bytes", extensionSize)
		}

		data := make([]byte, extensionSize)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
		h.extensions[extensionId] = data

		// Align to 8 bytes
		if extensionSize%8 != 0 {
			if _, err := r.Seek(int64(8-extensionSize&8), io.SeekCurrent); err != nil {
				return err
			}
		}
	}

	return nil
}

func (h *headerImpl) write() error {
	if h.v3.IncompatibleFeatures&featureDirty != 0 {
		return errors.New("Don't know how to write with dirty refcounts")
	}

	h.v3.AutoclearFeatures &= autoclearKnown

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, h.v2); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.BigEndian, h.v3); err != nil {
		return err
	}
	if h.extraHeader != nil {
		if _, err := buf.Write(h.extraHeader); err != nil {
			return err
		}
	}
	for extensionId, data := range h.extensions {
		if err := binary.Write(&buf, binary.BigEndian, extensionId); err != nil {
			return err
		}
		var extensionSize uint32 = uint32(len(data))
		if err := binary.Write(&buf, binary.BigEndian, extensionSize); err != nil {
			return err
		}
		if _, err := buf.Write(data); err != nil {
			return err
		}
		for ; extensionSize%8 != 0; extensionSize++ {
			if err := buf.WriteByte(0); err != nil {
				return err
			}
		}
	}
	var end uint32 = 0
	if err := binary.Write(&buf, binary.BigEndian, end); err != nil {
		return err
	}

	// Check the total size
	if buf.Len() > h.clusterSize() {
		return errors.New("Header too large")
	}

	// Write the header
	if _, err := h.ioAt.WriteAt(buf.Bytes(), 0); err != nil {
		return err
	}

	return nil
}

func (h *headerImpl) clusterSize() int {
	return 1 << h.v2.ClusterBits
}

func (h *headerImpl) size() int64 {
	return int64(h.v2.Size)
}

func (h *headerImpl) l1Size() int {
	return int(h.v2.L1Size)
}

func (h *headerImpl) l1Offset() int64 {
	return int64(h.v2.L1TableOffset)
}

func (*headerImpl) close() error {
	// Closing io is not our responsibility.
	return nil
}

func (h *headerImpl) io() ioAt {
	return h.ioAt
}
