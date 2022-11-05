package nexus

import (
	"github.com/squareup/pranadb/shakti/cmn"
)

type Conf struct {
	RegistryFormat                 cmn.MetadataFormat
	MasterRegistryRecordID         string
	L0FilesCompactionTrigger       int // There is one L0 per processor. This is the value per processor
	L1FilesCompactionTrigger       int // There is a single L1. This is a global value
	LevelFilesMultiplier           int // After L1, subsequence levels compaction trigger multiplies by this for each level
	MaxRegistrySegmentTableEntries int
	LogFileName                    string
}
