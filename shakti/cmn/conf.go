package cmn

import "time"

type Conf struct {
	MemtableMaxSizeBytes      int
	MemTableMaxReplaceTime    time.Duration
	MemtableFlushQueueMaxSize int
	TableFormat               DataFormat
}

type DataFormat byte

const (
	DataFormatV1 DataFormat = 1
)

type MetadataFormat byte

const (
	MetadataFormatV1 MetadataFormat = 1
)
