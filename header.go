package qcow2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/timtadh/data-structures/exc"
	"github.com/vasi/go-qcow2/bio"
)

type header interface {
	open(bio.ReaderWriterAt)
	close()

	write()
	autoclear()

	version() int
	clusterSize() int

	l1Entries() int
	l1Offset() int64
	size() int64

	refcountOffset() int64
	refcountClusters() int
	refcountBits() int
	setRefcountTable(offset int64, size int)

	snapshotsOffset() int64
	snapshotsCount() int

	io() *bio.BinaryIO
}

type featureType int

const (
	magic uint32 = 0x514649fb

	featureNameExtensionID uint32      = 0x6803f857
	incompatible           featureType = 0
	compatible             featureType = 1
	autoclear              featureType = 2

	featureDirty         uint64 = 1
	featureCorrupt       uint64 = 2
	incompatibleKnown    uint64 = featureDirty | featureCorrupt
	featureLazyRefcounts uint64 = 1
	featureBitmaps       uint64 = 1
	autoclearKnown       uint64 = featureBitmaps

	v2Length uint32 = 72
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

type featureName struct {
	ftype featureType
	bit   int
	name  string
}

type headerImpl struct {
	bio          *bio.BinaryIO
	v2           headerV2
	v3           headerV3
	extraHeader  []byte
	extensions   map[uint32][]byte
	featureNames []featureName
}

func (h *headerImpl) open(rw bio.ReaderWriterAt) {
	h.bio = bio.NewIO(rw, binary.BigEndian)

	// Validate some basic fields.
	h.bio.ReadData(0, &h.v2.Magic)
	if h.v2.Magic != magic {
		exc.Throwf("Not a qcow2 file")
	}
	h.bio.ReadData(4, &h.v2.Version)
	if h.v2.Version < 2 || h.v2.Version > 3 {
		exc.Throwf("Unsupported qcow2 format version %d", h.v2.Version)
	}
	h.bio.ReadData(20, &h.v2.ClusterBits)
	if h.v2.ClusterBits < 9 || h.v2.ClusterBits > 21 {
		exc.Throwf("Invalid qcow2 cluster bits %d", h.v2.ClusterBits)
	}

	// Make sure we don't read too far.
	section := bio.NewReaderSection(h.bio, 0, 1<<h.v2.ClusterBits)
	h.read(section)
}

func (h *headerImpl) close() {
	// Nothing to do
}

func (h *headerImpl) read(r *bio.SequentialReader) {
	// Read v2 header
	r.ReadData(&h.v2)

	// Validate fields
	if h.v2.BackingFileOffset != 0 {
		exc.Throwf("Backing files are not supported")
	}
	if h.v2.CryptMethod != 0 {
		exc.Throwf("Encryption is not supported")
	}

	guestBlocks := divceil(int64(h.v2.Size), int64(h.clusterSize()))
	l2Entries := h.clusterSize() / 8
	l1Entries := divceil(guestBlocks, int64(l2Entries))
	if l1Entries > int64(h.v2.L1Size) {
		exc.Throwf("Too few L1 entries for disk size")
	}

	if h.v2.L1TableOffset == 0 {
		exc.Throwf("Missing L1 table")
	}
	if h.v2.L1TableOffset%uint64(h.clusterSize()) != 0 {
		exc.Throwf("Unaligned L1 table")
	}
	if h.v2.RefcountTableOffset%uint64(h.clusterSize()) != 0 {
		exc.Throwf("Unaligned refcount table")
	}
	if h.v2.RefcountTableClusters == 0 || h.v2.RefcountTableOffset == 0 {
		exc.Throwf("Missing refcount table")
	}
	if h.v2.SnapshotsOffset%uint64(h.clusterSize()) != 0 {
		exc.Throwf("Unaligned snapshots")
	}

	if h.v2.Version == 2 {
		h.v3.IncompatibleFeatures = 0
		h.v3.CompatibleFeatures = 0
		h.v3.AutoclearFeatures = 0
		h.v3.RefcountOrder = 4
		h.v3.HeaderLength = v2Length
	} else {
		// Read v3 header
		r.ReadData(&h.v3)

		if h.v3.IncompatibleFeatures&featureCorrupt != 0 {
			exc.Throwf("Corrupt bit is set")
		}
		if h.v3.IncompatibleFeatures&featureDirty != 0 {
			exc.Throwf("Dirty bit is set")
		}

		if h.v3.RefcountOrder == 0 || h.v3.RefcountOrder > 6 {
			exc.Throwf("Bad refcount order %d", h.v3.RefcountOrder)
		}
	}

	// Read any extra header bytes.
	pos := r.Position()
	diff := int64(h.v3.HeaderLength) - pos
	if diff < 0 {
		exc.Throwf("Header data is longer than claimed length %d", h.v3.HeaderLength)
	} else if diff > 0 {
		h.extraHeader = make([]byte, diff)
		r.ReadBuf(h.extraHeader)
	}

	h.readExtensions(r)
	h.parseFeatureNames()
	h.checkIncompatibleFeatures()
}

func (h *headerImpl) readExtensions(r *bio.SequentialReader) {
	h.extensions = make(map[uint32][]byte)
	for {
		extensionID := r.ReadUint32()
		if extensionID == 0 {
			r.Align(8)
			break
		}

		extensionSize := r.ReadUint32()
		if int64(extensionSize) > r.Remain() {
			exc.Throwf("Extension too long, %d bytes", extensionSize)
		}

		data := make([]byte, extensionSize)
		r.ReadBuf(data)
		r.Align(8)
		h.extensions[extensionID] = data
	}
}

func (h *headerImpl) parseFeatureNames() {
	h.featureNames = make([]featureName, 0)
	data, found := h.extensions[featureNameExtensionID]
	if !found {
		return
	}

	for i := 0; i < len(data); i += 48 {
		name := data[i+2 : i+48]
		bytes.TrimRight(data, "\x00")
		h.featureNames = append(h.featureNames, featureName{
			featureType(data[i]),
			int(data[i+1]),
			string(name),
		})
	}
}

func (h *headerImpl) checkIncompatibleFeatures() {
	unknown := h.v3.IncompatibleFeatures &^ incompatibleKnown
	if unknown == 0 {
		return
	}

	names := make([]string, 0)
	for i := 0; i < 64 && unknown != 0; i++ {
		if unknown&1 != 0 {
			name := fmt.Sprintf("bit %d", i)
			for _, fn := range h.featureNames {
				if fn.ftype == incompatible && fn.bit == i {
					name = fn.name
					break
				}
			}
			names = append(names, name)
		}
		unknown >>= 1
	}

	exc.Throwf("Incompatible features: " + strings.Join(names, ", "))
}

func (h *headerImpl) write() {
	h.v3.AutoclearFeatures &= autoclearKnown

	w := bio.NewBinaryWriter(h.bio, 0)
	w.WriteData(h.v2)
	w.WriteData(h.v3)
	if h.extraHeader != nil {
		w.WriteBuf(h.extraHeader)
	}
	for extensionID, data := range h.extensions {
		w.WriteData(extensionID)
		w.WriteData(uint32(len(data)))
		w.WriteBuf(data)
		w.Align(8)
	}
	w.WriteData(uint32(0))
	w.Align(8)

	// Check the total size
	if w.Size() > h.clusterSize() {
		exc.Throwf("Header too large")
	}

	w.Commit()
}

func (h *headerImpl) autoclear() {
	if h.v3.AutoclearFeatures&^autoclearKnown == 0 {
		return
	}
	h.write()
}

func (h *headerImpl) clusterSize() int {
	return 1 << h.v2.ClusterBits
}

func (h *headerImpl) size() int64 {
	return int64(h.v2.Size)
}

func (h *headerImpl) l1Entries() int {
	return int(h.v2.L1Size)
}

func (h *headerImpl) l1Offset() int64 {
	return int64(h.v2.L1TableOffset)
}

func (h *headerImpl) io() *bio.BinaryIO {
	return h.bio
}

func (h *headerImpl) refcountOffset() int64 {
	return int64(h.v2.RefcountTableOffset)
}

func (h *headerImpl) refcountClusters() int {
	return int(h.v2.RefcountTableClusters)
}

func (h *headerImpl) refcountBits() int {
	return 1 << h.v3.RefcountOrder
}

func (h *headerImpl) setRefcountTable(offset int64, size int) {
	h.v2.RefcountTableOffset = uint64(offset)
	h.v2.RefcountTableClusters = uint32(size)
	h.write()
}

func (h *headerImpl) snapshotsOffset() int64 {
	return int64(h.v2.SnapshotsOffset)
}

func (h *headerImpl) snapshotsCount() int {
	return int(h.v2.NbSnapshots)
}

func (h *headerImpl) version() int {
	return int(h.v2.Version)
}
