package shakti

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

/*
Add a validate() method on registry that we can use to make sure L > 0 is non overlapping and in order

Test registering in L0 and getting.

Registering in different levels.

====

a) Test getFilesInrange

Set up multiple fils in L0, some with gaps between and others touching continuously and others completely overlapping
and others partially overlapping.

1. Restart reg keeping cloud store active and make sure it picks up existing master record
2. Multiple files in L0. Not found if range between two files
3. Multiple files in L0. Not found if range before first file
4. Multiple files in L0. Not found if range after last file.
5. Multiple files in L0. Test boundary conditions - start of range inclusive, end exclusive
6. Multiple files in L0. Found a single file - range exactly matching file.
6. Multiple files in L0. Found a single file - range completely inside file
6. Multiple files in L0. Found a single file - range larger than file and completely consuming it
6. Multiple files in L0. Found a single file - range over left boundary
6. Multiple files in L0. Found a single file - range over right boundary

As above but in L1 etc, test no found in different situations, and found in other situations

Test with files in multiple levels

b) Test apply changes

1. test apply changes error conditions, non existing tables etc
2. test adding single table to L0, verify it gets added on last segment
3. L0, verify new segment gets created when seg reaches max size
4. L0, test remove table from L0, test, not available for get any more
5. L0, remove last table from segment, what happens there? We can't have empty segments
6. L1, test add table, make sure it gets added in correct segment. Try this with table at beginning range of esisting segment
at end range, in the middle.
7. L1, add tables, make sure new segments are created when they reach maximum size
8. L1 delete tables, make sure can't find afterwards.
9 L1 delete tables until segments empty, what happens? Segments should get merged and deleted when empty

10. Delete multiple from L0 and add multiple to L1 in same batch
11. As above but higher levels, e.g. L2 to L3

13. Test flushing. Add manual flush we can call from tests. Make sure new segments and latest state is stored to cloud store
14. Make changes, flush, repeat, flush several times, restart registry from cloud store. Verify state is all there.
15. Add check on cloud store that there are no orphaned segments in cloud store after deleting.

16. Test recovery. Make changes, replicate but don't flush. Change leader, recover, make sure state is recovered, flushed
and no data lost.
17. As above but test case where new leader doesn't have the data as joined later and didn't get the previous flush. It should ask other nodes for data
and retrieve it from there.
 */

func TestSimple(t *testing.T) {
	conf := Conf{
		RegistryFormat:                 RegistryFormatV1,
		MasterRegistryRecordID:         "test_master_record",
		MaxRegistrySegmentTableEntries: 0,
		LogFileName:                    "test_reg.log",
	}
	cloudStore := &LocalCloudStore{}
	reg := NewRegistry(conf, cloudStore, &NoopReplicator{})
	err := reg.Start()
	require.NoError(t, err)
	err = reg.SetLeader()
	require.NoError(t, err)

	tableID := genTableID()

	regEntry := RegistrationEntry{
		Level:    0,
		TableID:  tableID,
		KeyStart: []byte("key002"),
		KeyEnd:   []byte("key007"),
	}
	regBatch := RegistrationBatch{
		Registrations:   []RegistrationEntry{regEntry},
	}
	err = reg.ApplyChanges(regBatch)
	require.NoError(t, err)

	overlapTabIDs, err := reg.GetTableIDsForRange([]byte("key001"), []byte("key008"), 10000)
	require.NoError(t, err)
	require.Equal(t, 1, len(overlapTabIDs))
	nonoverlapTabIds := overlapTabIDs[0]
	require.Equal(t, 1, len(nonoverlapTabIds))
	tabID := nonoverlapTabIds[0]

	require.Equal(t, tableID, tabID)

	err = reg.Validate(false)
	require.NoError(t, err)
}

func genTableID() SSTableID {
	id, err := uuid.New().MarshalBinary()
	if err != nil {
		panic(err)
	}
	return id
}

type NoopReplicator struct {
}

func (n NoopReplicator) ReplicateMessage(message []byte) error {
	return nil
}

