package datacontroller

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/shakti/cloudstore"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/sst"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

/*

16. Test recovery. Make changes, replicate but don't flush. Change leader, recover, make sure state is recovered, flushed
and no data lost.
17. As above but test case where new leader doesn't have the data as joined later and didn't get the previous flush. It should ask other nodes for data
and retrieve it from there.
*/

func TestAddAndGet_L0(t *testing.T) {
	cntrl := setupController(t)

	addedTableIDs := addTables(t, cntrl, 0,
		3, 5, 6, 7, 8, 12, //contiguous
		14, 17, 20, 21, 23, 30, //gaps between
		32, 40, 32, 40, 32, 40, //exactly overlapping
		42, 50, 45, 47, //one fully inside other
		51, 54, 52, 56, 54, 60) //each overlapping next one

	// Before data shouldn't find anything
	overlapTableIDs := getInRange(t, cntrl, 0, 1)
	validateTabIds(t, nil, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 0, 3)
	validateTabIds(t, nil, overlapTableIDs)

	// After data shouldn't find anything
	overlapTableIDs = getInRange(t, cntrl, 91, 93)
	validateTabIds(t, nil, overlapTableIDs)

	// Get entire contiguous block with exact range
	overlapTableIDs = getInRange(t, cntrl, 3, 13)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[0]},
		NonoverlappingTableIDs{addedTableIDs[1]},
		NonoverlappingTableIDs{addedTableIDs[2]},
	}, overlapTableIDs)

	// Get entire contiguous block with smaller range
	overlapTableIDs = getInRange(t, cntrl, 4, 10)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[0]},
		NonoverlappingTableIDs{addedTableIDs[1]},
		NonoverlappingTableIDs{addedTableIDs[2]},
	}, overlapTableIDs)

	// Get entire contiguous block with larger range
	overlapTableIDs = getInRange(t, cntrl, 2, 14)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[0]},
		NonoverlappingTableIDs{addedTableIDs[1]},
		NonoverlappingTableIDs{addedTableIDs[2]},
	}, overlapTableIDs)

	// Get exactly one from contiguous block
	overlapTableIDs = getInRange(t, cntrl, 6, 8)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[1]},
	}, overlapTableIDs)

	// Don't return anything from gap
	overlapTableIDs = getInRange(t, cntrl, 18, 20)
	validateTabIds(t, nil, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 18, 19)
	validateTabIds(t, nil, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 22, 23)
	validateTabIds(t, nil, overlapTableIDs)

	// Select entire block with gaps
	overlapTableIDs = getInRange(t, cntrl, 14, 31)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[3]},
		NonoverlappingTableIDs{addedTableIDs[4]},
		NonoverlappingTableIDs{addedTableIDs[5]},
	}, overlapTableIDs)

	// Select subset with gaps
	overlapTableIDs = getInRange(t, cntrl, 18, 22)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[4]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 18, 31)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[4]},
		NonoverlappingTableIDs{addedTableIDs[5]},
	}, overlapTableIDs)

	// Exactly overlapping
	overlapTableIDs = getInRange(t, cntrl, 32, 41)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[6]},
		NonoverlappingTableIDs{addedTableIDs[7]},
		NonoverlappingTableIDs{addedTableIDs[8]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 31, 41)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[6]},
		NonoverlappingTableIDs{addedTableIDs[7]},
		NonoverlappingTableIDs{addedTableIDs[8]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 35, 35)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[6]},
		NonoverlappingTableIDs{addedTableIDs[7]},
		NonoverlappingTableIDs{addedTableIDs[8]},
	}, overlapTableIDs)

	// Fully inside
	overlapTableIDs = getInRange(t, cntrl, 42, 48)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[9]},
		NonoverlappingTableIDs{addedTableIDs[10]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 41, 49)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[9]},
		NonoverlappingTableIDs{addedTableIDs[10]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 45, 46)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[9]},
		NonoverlappingTableIDs{addedTableIDs[10]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 41, 43)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[9]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 48, 49)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[9]},
	}, overlapTableIDs)

	// Select entire block overlapping next
	overlapTableIDs = getInRange(t, cntrl, 51, 61)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[11]},
		NonoverlappingTableIDs{addedTableIDs[12]},
		NonoverlappingTableIDs{addedTableIDs[13]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 53, 58)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[11]},
		NonoverlappingTableIDs{addedTableIDs[12]},
		NonoverlappingTableIDs{addedTableIDs[13]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 51, 53)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[11]},
		NonoverlappingTableIDs{addedTableIDs[12]},
	}, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 55, 56)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[12]},
		NonoverlappingTableIDs{addedTableIDs[13]},
	}, overlapTableIDs)

	afterTest(t, cntrl)
}

func TestAddAndGet_L1(t *testing.T) {
	cntrl := setupController(t)

	addedTableIDs := addTables(t, cntrl, 1,
		3, 5, 6, 8, 9, 11, 14, 15, 20, 30, 35, 50)

	// Before data shouldn't find anything
	overlapTableIDs := getInRange(t, cntrl, 0, 1)
	validateTabIds(t, nil, overlapTableIDs)
	overlapTableIDs = getInRange(t, cntrl, 0, 3)
	validateTabIds(t, nil, overlapTableIDs)

	// After data shouldn't find anything
	overlapTableIDs = getInRange(t, cntrl, 51, 56)
	validateTabIds(t, nil, overlapTableIDs)

	// Get entire block with exact range
	overlapTableIDs = getInRange(t, cntrl, 3, 51)
	validateTabIds(t, OverlappingTableIDs{addedTableIDs}, overlapTableIDs)

	// Get subset block with exact range
	overlapTableIDs = getInRange(t, cntrl, 6, 16)
	validateTabIds(t, OverlappingTableIDs{addedTableIDs[1:4]}, overlapTableIDs)

	// Get entire block with larger range
	overlapTableIDs = getInRange(t, cntrl, 0, 1000)
	validateTabIds(t, OverlappingTableIDs{addedTableIDs}, overlapTableIDs)

	// Get single table
	overlapTableIDs = getInRange(t, cntrl, 9, 11)
	validateTabIds(t, OverlappingTableIDs{{addedTableIDs[2]}}, overlapTableIDs)

	afterTest(t, cntrl)
}

func TestAddAndGet_MultipleLevels(t *testing.T) {
	cntrl := setupController(t)

	addedTableIDs0 := addTables(t, cntrl, 0,
		51, 54, 52, 56, 54, 60)

	addedTableIDs1 := addTables(t, cntrl, 1,
		48, 49, 52, 54, 55, 58, 59, 63)

	addedTableIDs2 := addTables(t, cntrl, 2,
		22, 31, 38, 48, 50, 52, 53, 65, 68, 70)

	// Get everything
	overlapTableIDs := getInRange(t, cntrl, 20, 70)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs0[0]},
		NonoverlappingTableIDs{addedTableIDs0[1]},
		NonoverlappingTableIDs{addedTableIDs0[2]},
		addedTableIDs1,
		addedTableIDs2,
	}, overlapTableIDs)

	// Get selection
	overlapTableIDs = getInRange(t, cntrl, 55, 58)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs0[1]},
		NonoverlappingTableIDs{addedTableIDs0[2]},
		addedTableIDs1[2:3],
		addedTableIDs2[3:4],
	}, overlapTableIDs)

	afterTest(t, cntrl)
}

func TestAddAndGetAll_MultipleSegments(t *testing.T) {
	cntrl := setupControllerWithMaxEntries(t, 100)

	numEntries := 1000
	var pairs []int
	ks := 0
	for i := 0; i < numEntries; i++ {
		pairs = append(pairs, ks, ks+1)
		ks += 3
	}
	addedTableIDs := addTables(t, cntrl, 1, pairs...)

	mr := cntrl.getMasterRecord()
	// Should be 10 segments
	require.Equal(t, 10, len(mr.levelSegmentEntries[1]))

	// Now get them all, spanning multiple segments
	oids, err := cntrl.GetTableIDsForRange(nil, nil, 1000000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, addedTableIDs, []sst.SSTableID(ids))
}

func TestAddAndGetMost_MultipleSegments(t *testing.T) {
	cntrl := setupControllerWithMaxEntries(t, 100)

	numEntries := 1000
	var pairs []int
	ks := 0
	for i := 0; i < numEntries; i++ {
		pairs = append(pairs, ks, ks+1)
		ks += 2
	}
	addedTableIDs := addTables(t, cntrl, 1, pairs...)

	mr := cntrl.getMasterRecord()
	// Should be 10 segments
	require.Equal(t, 10, len(mr.levelSegmentEntries[1]))

	// Now get most of them, spanning multiple segments
	oids, err := cntrl.GetTableIDsForRange(createKey(2), createKey(2*(numEntries-2)), 1000000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	expected := addedTableIDs[1 : len(addedTableIDs)-2]
	require.Equal(t, expected, []sst.SSTableID(ids))
}

func TestAddAndRemove_L0(t *testing.T) {
	cntrl := setupController(t)

	addedTableIDs := addTables(t, cntrl, 0,
		2, 4, 5, 7, 14, 17, 20, 21, 23, 30)

	overlapTableIDs := getInRange(t, cntrl, 2, 31)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[0]},
		NonoverlappingTableIDs{addedTableIDs[1]},
		NonoverlappingTableIDs{addedTableIDs[2]},
		NonoverlappingTableIDs{addedTableIDs[3]},
		NonoverlappingTableIDs{addedTableIDs[4]},
	}, overlapTableIDs)

	deregEntry0 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[0][0],
		KeyStart: createKey(2),
		KeyEnd:   createKey(4),
	}
	deregBatch := RegistrationBatch{
		Deregistrations: []RegistrationEntry{deregEntry0},
	}
	err := cntrl.ApplyChanges(deregBatch)
	require.NoError(t, err)

	overlapTableIDs2 := getInRange(t, cntrl, 2, 31)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[1]},
		NonoverlappingTableIDs{addedTableIDs[2]},
		NonoverlappingTableIDs{addedTableIDs[3]},
		NonoverlappingTableIDs{addedTableIDs[4]},
	}, overlapTableIDs2)

	deregEntry2 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[2][0],
		KeyStart: createKey(14),
		KeyEnd:   createKey(17),
	}

	deregEntry4 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[4][0],
		KeyStart: createKey(23),
		KeyEnd:   createKey(30),
	}

	deregBatch2 := RegistrationBatch{
		Deregistrations: []RegistrationEntry{deregEntry2, deregEntry4},
	}
	err = cntrl.ApplyChanges(deregBatch2)
	require.NoError(t, err)

	overlapTableIDs3 := getInRange(t, cntrl, 2, 31)
	validateTabIds(t, OverlappingTableIDs{
		NonoverlappingTableIDs{addedTableIDs[1]},
		NonoverlappingTableIDs{addedTableIDs[3]},
	}, overlapTableIDs3)

	deregEntry1 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[1][0],
		KeyStart: createKey(5),
		KeyEnd:   createKey(7),
	}

	deregEntry3 := RegistrationEntry{
		Level:    0,
		TableID:  overlapTableIDs[3][0],
		KeyStart: createKey(20),
		KeyEnd:   createKey(21),
	}

	deregBatch3 := RegistrationBatch{
		Deregistrations: []RegistrationEntry{deregEntry1, deregEntry3},
	}
	err = cntrl.ApplyChanges(deregBatch3)
	require.NoError(t, err)

	afterTest(t, cntrl)
}

func TestAddRemove_L0(t *testing.T) {
	cntrl := setupControllerWithMaxEntries(t, 10)

	mr := cntrl.getMasterRecord()
	require.Equal(t, cmn.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(0), mr.version)
	require.Equal(t, 0, len(mr.levelSegmentEntries))

	// Add some table entries in level 0

	tableIDs1 := addTables(t, cntrl, 0,
		12, 17, 3, 9, 1, 2, 10, 15, 4, 20, 7, 30)

	mr = cntrl.getMasterRecord()
	require.Equal(t, cmn.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(1), mr.version)
	require.Equal(t, 1, len(mr.levelSegmentEntries))

	segEntries := mr.levelSegmentEntries[0]
	require.NotNil(t, segEntries)
	require.Equal(t, 1, len(segEntries))
	segEntry := segEntries[0]
	require.Equal(t, createKey(1), segEntry.rangeStart)
	require.Equal(t, createKey(30), segEntry.rangeEnd)

	// Add some more - now there should be 10

	tableIDs2 := addTables(t, cntrl, 0,
		11, 13, 3, 9, 0, 35, 7, 12)

	mr = cntrl.getMasterRecord()
	require.Equal(t, cmn.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(2), mr.version)
	require.Equal(t, 1, len(mr.levelSegmentEntries))

	segEntries = mr.levelSegmentEntries[0]
	require.NotNil(t, segEntries)
	require.Equal(t, 1, len(segEntries))
	segEntry = segEntries[0]
	require.Equal(t, createKey(0), segEntry.rangeStart)
	require.Equal(t, createKey(35), segEntry.rangeEnd)

	// Add some more, should still be one segment as L0 only ever has one segment

	tableIDs3 := addTables(t, cntrl, 0,
		15, 19, 45, 47, 12, 13, 88, 89, 45, 40)

	mr = cntrl.getMasterRecord()
	require.Equal(t, cmn.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(3), mr.version)
	require.Equal(t, 1, len(mr.levelSegmentEntries))

	segEntries = mr.levelSegmentEntries[0]
	require.NotNil(t, segEntries)
	require.Equal(t, 1, len(segEntries))
	segEntry = segEntries[0]
	require.Equal(t, createKey(0), segEntry.rangeStart)
	require.Equal(t, createKey(89), segEntry.rangeEnd)

	// Now delete some

	removeTables(t, cntrl, 0, tableIDs3, 15, 19, 45, 47, 12, 13, 88, 89, 45, 40)

	mr = cntrl.getMasterRecord()
	require.Equal(t, cmn.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(4), mr.version)
	require.Equal(t, 1, len(mr.levelSegmentEntries))

	segEntries = mr.levelSegmentEntries[0]
	require.NotNil(t, segEntries)
	require.Equal(t, 1, len(segEntries))
	segEntry = segEntries[0]
	require.Equal(t, createKey(0), segEntry.rangeStart)
	require.Equal(t, createKey(35), segEntry.rangeEnd)

	removeTables(t, cntrl, 0, tableIDs2, 11, 13, 3, 9, 0, 35, 7, 12)

	mr = cntrl.getMasterRecord()
	require.Equal(t, cmn.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(5), mr.version)
	require.Equal(t, 1, len(mr.levelSegmentEntries))

	segEntries = mr.levelSegmentEntries[0]
	require.NotNil(t, segEntries)
	require.Equal(t, 1, len(segEntries))
	segEntry = segEntries[0]
	require.Equal(t, createKey(1), segEntry.rangeStart)
	require.Equal(t, createKey(30), segEntry.rangeEnd)

	removeTables(t, cntrl, 0, tableIDs1, 12, 17, 3, 9, 1, 2, 10, 15, 4, 20, 7, 30)

	// Should be all gone
	mr = cntrl.getMasterRecord()
	require.Equal(t, cmn.MetadataFormatV1, mr.format)
	require.Equal(t, uint64(6), mr.version)
	require.Equal(t, 0, len(mr.levelSegmentEntries))

	afterTest(t, cntrl)
}

func TestRemoveUnknownTableL0(t *testing.T) {
	testRemoveUnknownTable(t, 0)
}

func TestRemoveUnknownTableL(t *testing.T) {
	testRemoveUnknownTable(t, 1)
}

func testRemoveUnknownTable(t *testing.T, level int) {
	t.Helper()
	cntrl := setupControllerWithMaxEntries(t, 10)

	addTables(t, cntrl, level, 3, 5, 6, 7, 8, 12)

	unknownTableID, err := uuid.New().MarshalBinary()
	require.NoError(t, err)

	regBatch := RegistrationBatch{
		Deregistrations: []RegistrationEntry{{
			Level:    level,
			TableID:  unknownTableID,
			KeyStart: createKey(6),
			KeyEnd:   createKey(7),
		}},
	}
	err = cntrl.ApplyChanges(regBatch)
	require.Error(t, err, "cannot find table entry in segment")
	afterTest(t, cntrl)
}

func TestAddAndRemoveSameBatch(t *testing.T) {
	cntrl := setupControllerWithMaxEntries(t, 10)

	tableIDs := addTables(t, cntrl, 1, 3, 5, 6, 7, 8, 12)

	newTableID1, err := uuid.New().MarshalBinary()
	require.NoError(t, err)
	newTableID2, err := uuid.New().MarshalBinary()
	require.NoError(t, err)

	regBatch := RegistrationBatch{
		Registrations: []RegistrationEntry{{
			Level:    1,
			TableID:  newTableID1,
			KeyStart: createKey(15),
			KeyEnd:   createKey(19),
		}, {
			Level:    1,
			TableID:  newTableID2,
			KeyStart: createKey(36),
			KeyEnd:   createKey(39),
		}},
		Deregistrations: []RegistrationEntry{{
			Level:    1,
			TableID:  tableIDs[1],
			KeyStart: createKey(6),
			KeyEnd:   createKey(7),
		}},
	}
	err = cntrl.ApplyChanges(regBatch)
	require.NoError(t, err)

	oTabIDs, err := cntrl.GetTableIDsForRange(createKey(3), createKey(1000), 10000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oTabIDs))
	noTabIDs := oTabIDs[0]
	require.Equal(t, 4, len(noTabIDs))

	require.Equal(t, tableIDs[0], noTabIDs[0])
	require.Equal(t, tableIDs[2], noTabIDs[1])
	require.Equal(t, sst.SSTableID(newTableID1), noTabIDs[2])
	require.Equal(t, sst.SSTableID(newTableID2), noTabIDs[3])

	afterTest(t, cntrl)
}

func TestNilRangeStartAndEnd(t *testing.T) {
	// Rqnge start of nil means start at the beginning
	testNilRangeStartAndEnd(t, nil, createKey(1000))
	// Range end of nil means right to the end
	testNilRangeStartAndEnd(t, createKey(0), nil)
	// Full range
	testNilRangeStartAndEnd(t, nil, nil)
}

func testNilRangeStartAndEnd(t *testing.T, rangeStart []byte, rangeEnd []byte) {
	t.Helper()
	cntrl := setupControllerWithMaxEntries(t, 10)

	addTables(t, cntrl, 1, 3, 5, 6, 7, 8, 12)

	oTabIDs, err := cntrl.GetTableIDsForRange(rangeStart, rangeEnd, 10000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oTabIDs))
	noTabIDs := oTabIDs[0]
	require.Equal(t, 3, len(noTabIDs))
}

func TestAddRemoveL1_OrderedKeys(t *testing.T) {
	testAddRemoveNonOverlapping(t, true, 1, 0, 1)
	testAddRemoveNonOverlapping(t, true, 1, 1, 1)
	testAddRemoveNonOverlapping(t, true, 1, 0, 3)
	testAddRemoveNonOverlapping(t, true, 3, 0, 1)
}

func TestAddRemoveL1_RandomKeys(t *testing.T) {
	testAddRemoveNonOverlapping(t, false, 1, 0, 1)
	testAddRemoveNonOverlapping(t, false, 1, 1, 1)
	testAddRemoveNonOverlapping(t, false, 1, 0, 3)
	testAddRemoveNonOverlapping(t, false, 3, 0, 1)
}

func testAddRemoveNonOverlapping(t *testing.T, ordered bool, level int, rangeGap int, batchSize int) {
	t.Helper()
	maxTableEntries := 100
	numEntries := 10000
	cntrl := setupControllerWithMaxEntries(t, maxTableEntries)

	entries := prepareEntries(numEntries, 2, rangeGap)
	if !ordered {
		entries = shuffleEntries(entries)
	}

	var regBatch *RegistrationBatch
	entryNum := 0
	batchNum := 0
	for entryNum < len(entries) {
		if regBatch == nil {
			regBatch = &RegistrationBatch{}
		}
		ent := entries[entryNum]
		tableID, err := uuid.New().MarshalBinary()
		require.NoError(t, err)
		regBatch.Registrations = append(regBatch.Registrations, RegistrationEntry{
			Level:    level,
			TableID:  tableID,
			KeyStart: ent.keyStart,
			KeyEnd:   ent.keyEnd,
		})
		if len(regBatch.Registrations) == batchSize || entryNum == len(entries)-1 {
			err = cntrl.ApplyChanges(*regBatch)
			require.NoError(t, err)
			// Get the table ids to make sure they were added ok
			for i := 0; i < len(regBatch.Registrations); i++ {
				oIDs, err := cntrl.GetTableIDsForRange(regBatch.Registrations[i].KeyStart,
					common.IncrementBytesBigEndian(regBatch.Registrations[i].KeyEnd), 1000000)
				require.NoError(t, err)
				require.Equal(t, 1, len(oIDs))
				nIDs := oIDs[0]
				require.Equal(t, 1, len(nIDs))
				require.Equal(t, regBatch.Registrations[i].TableID, nIDs[0])
				// We store the sstableid on the entry so we can use it later in the deregistration
				entries[entryNum+1-len(regBatch.Registrations)+i].tableID = nIDs[0]
			}
			checkState(t, cntrl, entryNum+1, batchNum, level, maxTableEntries, ordered, false)
			batchNum++
			regBatch = nil
		}
		entryNum++
	}
	regBatch = nil

	// Now shuffle the entries and remove in batches
	// verify removed entries can't be getted
	if !ordered {
		entries = shuffleEntries(entries)
	}
	entryNum = 0
	for entryNum < len(entries) {
		if regBatch == nil {
			regBatch = &RegistrationBatch{}
		}
		ent := entries[entryNum]

		regBatch.Deregistrations = append(regBatch.Deregistrations, RegistrationEntry{
			Level:    level,
			TableID:  ent.tableID,
			KeyStart: ent.keyStart,
			KeyEnd:   ent.keyEnd,
		})
		if len(regBatch.Deregistrations) == batchSize || entryNum == len(entries)-1 {
			err := cntrl.ApplyChanges(*regBatch)
			require.NoError(t, err)
			// Get the table ids to make sure they were removed ok
			for i := 0; i < len(regBatch.Deregistrations); i++ {
				oIDs, err := cntrl.GetTableIDsForRange(regBatch.Deregistrations[i].KeyStart,
					common.IncrementBytesBigEndian(regBatch.Deregistrations[i].KeyEnd), 1000000)
				require.NoError(t, err)
				require.Equal(t, 0, len(oIDs))
			}
			entriesLeft := len(entries) - entryNum - 1
			checkState(t, cntrl, entriesLeft, batchNum, level, maxTableEntries, ordered, true)
			batchNum++
			regBatch = nil
		}
		entryNum++
	}
}

func shuffleEntries(entries []entry) []entry {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
	return entries
}

func checkState(t *testing.T, cntrl *controller, numEntries int, batchNum int, level int, maxTableEntries int, ordered bool, deleting bool) {
	t.Helper()
	err := cntrl.Validate(false)
	require.NoError(t, err)
	mr := cntrl.getMasterRecord()
	require.NotNil(t, mr)
	require.Equal(t, batchNum+1, int(mr.version))

	segEntries := mr.levelSegmentEntries[level]
	if numEntries == 0 {
		require.Nil(t, segEntries)
		return
	}
	if deleting {
		// When deleting most of the segments hang around until the end, as that's when their last entry is deleted
		return
	}

	require.NotNil(t, segEntries)

	// perfectNumSegs is what we would expect if all entries are added in key order and segments fill up one by one
	perfectNumSegs := (numEntries-1)/maxTableEntries + 1

	// However in the general case:
	// Each seg could possibly have one segment in between
	// Consider the following
	// We attempt to insert an entry in a segment but it is already full, the next segment is full too
	// so we create a new segment between them
	// So, in the worst case, the upper bound of number of segments is:
	maxSegs := 2*perfectNumSegs - 1
	// With incrementing keys all segments will fill from left to right and this will give smallest number of segments
	// With keys arriving in random order the number of segments will be somewhere between perfectNumSegs and maxSegs
	// It works out that we need approx 30% extra segments than if they were all perfectly packed

	if ordered {
		require.LessOrEqual(t, len(segEntries), perfectNumSegs)
	} else {
		require.LessOrEqual(t, len(segEntries), maxSegs)
	}
}

func prepareEntries(numEntries int, rangeSize int, rangeGap int) []entry {
	entries := make([]entry, 0, numEntries)
	start := 0
	for i := 0; i < numEntries; i++ {
		ks := createKey(start)
		ke := createKey(start + rangeSize - 1)
		start += rangeSize + rangeGap
		entries = append(entries, entry{
			keyStart: ks,
			keyEnd:   ke,
		})
	}
	return entries
}

type entry struct {
	keyStart []byte
	keyEnd   []byte
	tableID  sst.SSTableID
}

func afterTest(t *testing.T, cntrl Controller) {
	t.Helper()
	err := cntrl.Validate(false)
	require.NoError(t, err)
}

func validateTabIds(t *testing.T, expectedTabIDs OverlappingTableIDs, actualTabIDs OverlappingTableIDs) {
	t.Helper()
	require.Equal(t, expectedTabIDs, actualTabIDs)
}

func getInRange(t *testing.T, cntrl Controller, ks int, ke int) OverlappingTableIDs {
	t.Helper()
	keyStart := createKey(ks)
	keyEnd := createKey(ke)
	overlapTabIDs, err := cntrl.GetTableIDsForRange(keyStart, keyEnd, 10000)
	require.NoError(t, err)
	return overlapTabIDs
}

func setupController(t *testing.T) *controller {
	t.Helper()
	return setupControllerWithMaxEntries(t, 10000)
}

func setupControllerWithMaxEntries(t *testing.T, maxSegmentTableEntries int) *controller {
	t.Helper()
	conf := Conf{
		RegistryFormat:                 cmn.MetadataFormatV1,
		MasterRegistryRecordID:         "test_master_record",
		MaxRegistrySegmentTableEntries: maxSegmentTableEntries,
		LogFileName:                    "test_reg.log",
	}
	cloudStore := &cloudstore.LocalStore{}
	cntrl := newController(conf, cloudStore, &cmn.NoopReplicator{})
	err := cntrl.Start()
	require.NoError(t, err)
	err = cntrl.SetLeader()
	require.NoError(t, err)
	return cntrl
}

func createKey(i int) []byte {
	return []byte(fmt.Sprintf("prefix/key-%010d", i))
}

func removeTables(t *testing.T, cntrl Controller, level int, tabIDs []sst.SSTableID, pairs ...int) {
	t.Helper()
	var regEntries []RegistrationEntry
	j := 0
	for i := 0; i < len(pairs); i++ {
		ks := pairs[i]
		i++
		ke := pairs[i]
		firstKey := createKey(ks)
		lastKey := createKey(ke)
		tabID := tabIDs[j]
		j++
		regEntry := RegistrationEntry{
			Level:    level,
			TableID:  tabID,
			KeyStart: firstKey,
			KeyEnd:   lastKey,
		}
		regEntries = append(regEntries, regEntry)
	}
	regBatch := RegistrationBatch{
		Deregistrations: regEntries,
	}
	err := cntrl.ApplyChanges(regBatch)
	require.NoError(t, err)
}

func addTables(t *testing.T, cntrl Controller, level int, pairs ...int) []sst.SSTableID {
	t.Helper()
	addRegEntries, addTableIDs := createRegistrationEntries(t, level, pairs...)
	regBatch := RegistrationBatch{
		Registrations: addRegEntries,
	}
	err := cntrl.ApplyChanges(regBatch)
	require.NoError(t, err)
	return addTableIDs
}

func createRegistrationEntries(t *testing.T, level int, pairs ...int) ([]RegistrationEntry, []sst.SSTableID) {
	t.Helper()
	var regEntries []RegistrationEntry
	var tableIDs []sst.SSTableID
	for i := 0; i < len(pairs); i++ {
		ks := pairs[i]
		i++
		ke := pairs[i]
		firstKey := createKey(ks)
		lastKey := createKey(ke)
		tabID, err := uuid.New().MarshalBinary()
		require.NoError(t, err)
		regEntry := RegistrationEntry{
			Level:    level,
			TableID:  tabID,
			KeyStart: firstKey,
			KeyEnd:   lastKey,
		}
		regEntries = append(regEntries, regEntry)
		tableIDs = append(tableIDs, tabID)
	}
	return regEntries, tableIDs
}

func TestDataResetOnRestartWithoutFlush(t *testing.T) {
	cntrl := setupController(t)
	tabIDs := addTables(t, cntrl, 1, 3, 5, 6, 7, 8, 12)
	oids, err := cntrl.GetTableIDsForRange(nil, nil, 100000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, tabIDs, []sst.SSTableID(ids))

	err = cntrl.Stop()
	require.NoError(t, err)

	err = cntrl.Start()
	require.NoError(t, err)

	oids, err = cntrl.GetTableIDsForRange(nil, nil, 100000)
	require.NoError(t, err)
	require.Nil(t, oids)
}

func TestDataRestoredOnRestartWithFlush(t *testing.T) {
	cntrl := setupController(t)
	tabIDs := addTables(t, cntrl, 1, 3, 5, 6, 7, 8, 12)
	oids, err := cntrl.GetTableIDsForRange(nil, nil, 100000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, tabIDs, []sst.SSTableID(ids))

	_, _, err = cntrl.Flush()
	require.NoError(t, err)
	ver := cntrl.getMasterRecord().version

	err = cntrl.Stop()
	require.NoError(t, err)
	err = cntrl.Start()
	require.NoError(t, err)

	oids, err = cntrl.GetTableIDsForRange(nil, nil, 100000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids = oids[0]
	require.Equal(t, tabIDs, []sst.SSTableID(ids))

	ver2 := cntrl.getMasterRecord().version
	require.Equal(t, ver, ver2)
}

func TestFlushManyAdds(t *testing.T) {
	cntrl := setupControllerWithMaxEntries(t, 100)
	numAdds := 1000
	ks := 0
	var allTabIDs []sst.SSTableID
	for i := 0; i < numAdds; i++ {
		tabIDs := addTables(t, cntrl, 1, ks, ks+1)
		ks += 3
		allTabIDs = append(allTabIDs, tabIDs...)
	}

	oids, err := cntrl.GetTableIDsForRange(nil, nil, 100000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, allTabIDs, []sst.SSTableID(ids))

	mr := cntrl.getMasterRecord()
	ver := mr.version
	require.Equal(t, numAdds, int(ver))
	// Should be 10 segments
	require.Equal(t, 10, len(mr.levelSegmentEntries[1]))

	segsAdded, segsDeleted, err := cntrl.Flush()
	require.NoError(t, err)
	require.Equal(t, 10, segsAdded)
	require.Equal(t, 0, segsDeleted)

	err = cntrl.Stop()
	require.NoError(t, err)
	err = cntrl.Start()
	require.NoError(t, err)

	oids, err = cntrl.GetTableIDsForRange(nil, nil, 100000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids = oids[0]
	require.Equal(t, allTabIDs, []sst.SSTableID(ids))

	ver2 := cntrl.getMasterRecord().version
	require.Equal(t, ver, ver2)
}

func TestFlushManyAddsAndSomeDeletes(t *testing.T) {
	cntrl := setupControllerWithMaxEntries(t, 100)
	numAdds := 1000
	ks := 0
	var allTabIDs []sst.SSTableID
	for i := 0; i < numAdds; i++ {
		tabIDs := addTables(t, cntrl, 1, ks, ks+1)
		ks += 3
		allTabIDs = append(allTabIDs, tabIDs...)
	}
	mr := cntrl.getMasterRecord()
	ver := mr.version
	require.Equal(t, numAdds, int(ver))
	// Should be 10 segments
	require.Equal(t, 10, len(mr.levelSegmentEntries[1]))

	// Now we delete enough to reduce number of segments to 9
	numDels := 100
	var deregEntries []RegistrationEntry
	ks = 0
	for i := 0; i < numDels; i++ {
		deregEntries = append(deregEntries, RegistrationEntry{
			Level:    1,
			TableID:  allTabIDs[i],
			KeyStart: createKey(ks),
			KeyEnd:   createKey(ks + 1),
		})
		ks += 3
	}
	err := cntrl.ApplyChanges(RegistrationBatch{
		Deregistrations: deregEntries,
	})
	require.NoError(t, err)

	mr = cntrl.getMasterRecord()
	// Should be 9 segments
	require.Equal(t, 9, len(mr.levelSegmentEntries[1]))

	ver = mr.version
	require.Equal(t, numAdds+1, int(ver))

	// Even though we created 10 and deleted one segment, overall we just pushed 9 adds
	segsAdded, segsDeleted, err := cntrl.Flush()
	require.NoError(t, err)
	require.Equal(t, 9, segsAdded)
	require.Equal(t, 0, segsDeleted)

	err = cntrl.Stop()
	require.NoError(t, err)
	err = cntrl.Start()
	require.NoError(t, err)

	oids, err := cntrl.GetTableIDsForRange(nil, nil, 100000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, allTabIDs[100:], []sst.SSTableID(ids))

	ver2 := cntrl.getMasterRecord().version
	require.Equal(t, ver, ver2)
}

func TestFlushManyDeletes(t *testing.T) {
	// First add a bunch
	cntrl := setupControllerWithMaxEntries(t, 100)
	numAdds := 1000
	ks := 0
	var allTabIDs []sst.SSTableID
	for i := 0; i < numAdds; i++ {
		tabIDs := addTables(t, cntrl, 1, ks, ks+1)
		ks += 3
		allTabIDs = append(allTabIDs, tabIDs...)
	}
	mr := cntrl.getMasterRecord()
	ver := mr.version
	require.Equal(t, numAdds, int(ver))
	// Should be 10 segments
	require.Equal(t, 10, len(mr.levelSegmentEntries[1]))
	// Now flush
	segsAdded, segsDeleted, err := cntrl.Flush()
	require.NoError(t, err)
	require.Equal(t, 10, segsAdded)
	require.Equal(t, 0, segsDeleted)

	// Now we delete enough to reduce number of segments to 9
	numDels := 200
	var deregEntries []RegistrationEntry
	ks = 0
	for i := 0; i < numDels; i++ {
		deregEntries = append(deregEntries, RegistrationEntry{
			Level:    1,
			TableID:  allTabIDs[i],
			KeyStart: createKey(ks),
			KeyEnd:   createKey(ks + 1),
		})
		ks += 3
	}
	err = cntrl.ApplyChanges(RegistrationBatch{
		Deregistrations: deregEntries,
	})
	require.NoError(t, err)

	mr = cntrl.getMasterRecord()
	ver = mr.version
	require.Equal(t, numAdds+1, int(ver))
	// Should be 8 segments
	require.Equal(t, 8, len(mr.levelSegmentEntries[1]))

	segsAdded, segsDeleted, err = cntrl.Flush()
	require.NoError(t, err)
	require.Equal(t, 0, segsAdded)
	require.Equal(t, 2, segsDeleted)

	err = cntrl.Stop()
	require.NoError(t, err)
	err = cntrl.Start()
	require.NoError(t, err)

	oids, err := cntrl.GetTableIDsForRange(nil, nil, 100000)
	require.NoError(t, err)
	require.Equal(t, 1, len(oids))
	ids := oids[0]
	require.Equal(t, allTabIDs[200:], []sst.SSTableID(ids))

	ver2 := cntrl.getMasterRecord().version
	require.Equal(t, ver, ver2)
}

func TestMultipleFlushes(t *testing.T) {
	// First add a bunch
	cntrl := setupControllerWithMaxEntries(t, 100)
	numAdds := 1000

	ks := 0
	var allTabIDs []sst.SSTableID
	for i := 0; i < numAdds; i++ {

		tabIDs := addTables(t, cntrl, 1, ks, ks+1)
		ks += 3
		allTabIDs = append(allTabIDs, tabIDs...)

		mr := cntrl.getMasterRecord()
		ver := mr.version
		require.Equal(t, i+1, int(ver))

		oids, err := cntrl.GetTableIDsForRange(nil, nil, 100000)
		require.NoError(t, err)
		require.Equal(t, 1, len(oids))
		ids := oids[0]
		require.Equal(t, allTabIDs, []sst.SSTableID(ids))

		_, _, err = cntrl.Flush()
		require.NoError(t, err)

		err = cntrl.Stop()
		require.NoError(t, err)
		err = cntrl.Start()
		require.NoError(t, err)

		mr = cntrl.getMasterRecord()
		ver = mr.version
		require.Equal(t, i+1, int(ver))

		oids, err = cntrl.GetTableIDsForRange(nil, nil, 100000)
		require.NoError(t, err)
		require.Equal(t, 1, len(oids))
		ids = oids[0]
		require.Equal(t, allTabIDs, []sst.SSTableID(ids))
	}
}
