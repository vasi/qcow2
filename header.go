package qcow2

const magic uint32 = 0x514649fb

type headerV2 struct {
	Magic                   uint32
	Version                 uint32
	Backing_file_offset     uint64
	Cluster_bits            uint32
	Size                    uint64
	Crypt_method            uint32
	L1_size                 uint32
	L1_table_offset         uint64
	Refcount_table_offset   uint64
	Refcount_table_clusters uint32
	Nb_snapshots            uint32
	Snapshots_offset        uint64
}
