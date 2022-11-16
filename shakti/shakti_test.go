package shakti

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/shakti/cloudstore"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/datacontroller"
	"github.com/squareup/pranadb/shakti/iteration"
	"github.com/squareup/pranadb/shakti/mem"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

/*
TODO

1. Add lots of random keys data such that it creates multiple sstables in cloud store, then iterate the whole lot
2. Add lots of random keys data as fast as we can until it creates lots of sstables in cloud store and at same time, iterate
through the whole lot and make sure all is received.
3. Add some latency on cloud store and do above again, verify that memtable flush queue doesn't get too big
4. Like 2 but multiple concurent iterators
5. Instead of random keys, add lots of sequential data, with multiple iterators reading a prefix of that - similar to how
Kafka consumers would work
*/

func init() {
	log.SetLevel(log.TraceLevel)
}

func TestSimpleIterate(t *testing.T) {
	shakti := setupShakti(t)

	iter, err := shakti.NewIterator(nil, nil)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, shakti, 0, 10)
	iteratePairs(t, iter, 0, 10)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateAllThenAddMore(t *testing.T) {
	shakti := setupShakti(t)

	iter, err := shakti.NewIterator(nil, nil)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, shakti, 0, 10)
	iteratePairs(t, iter, 0, 10)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, shakti, 10, 10)
	iteratePairs(t, iter, 10, 10)
}

func TestIterateInRange(t *testing.T) {
	shakti := setupShakti(t)

	ks := []byte(fmt.Sprintf("prefix/key-%010d", 3))
	ke := []byte(fmt.Sprintf("prefix/key-%010d", 7))

	iter, err := shakti.NewIterator(ks, ke)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, shakti, 0, 10)
	iteratePairs(t, iter, 3, 4)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateInRangeNoEndRange(t *testing.T) {
	shakti := setupShakti(t)

	ks := []byte(fmt.Sprintf("prefix/key-%010d", 3))

	iter, err := shakti.NewIterator(ks, nil)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, shakti, 0, 10)
	iteratePairs(t, iter, 3, 7)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateThenDelete(t *testing.T) {
	shakti := setupShakti(t)

	iter, err := shakti.NewIterator(nil, nil)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, shakti, 0, 10)
	iteratePairs(t, iter, 0, 5)

	requireIterValid(t, iter, true)

	// Then we delete the rest of them from the memtable
	writeKVsWithTombstones(t, shakti, 5, 5)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateThenReplaceThenDelete(t *testing.T) {
	shakti := setupShakti(t)

	iter, err := shakti.NewIterator(nil, nil)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, shakti, 0, 10)
	iteratePairs(t, iter, 0, 5)
	requireIterValid(t, iter, true)

	err = shakti.forceReplaceMemtable()
	require.NoError(t, err)
	requireIterValid(t, iter, true)

	// Then we delete the rest of them from the memtable
	writeKVsWithTombstones(t, shakti, 5, 5)
	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateAllMemtableThenReplace(t *testing.T) {
	shakti := setupShakti(t)

	iter, err := shakti.NewIterator(nil, nil)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	// fully iterate through the memtable, then force a replace then fully iterate again. and repeat

	ks := 0
	for i := 0; i < 10; i++ {
		writeKVs(t, shakti, ks, 10)
		iteratePairs(t, iter, ks, 10)
		err = iter.Next()
		require.NoError(t, err)
		requireIterValid(t, iter, false)

		err = shakti.forceReplaceMemtable()
		require.NoError(t, err)
		ks += 10
	}
}

func TestIterateSomeMemtableThenReplace(t *testing.T) {
	shakti := setupShakti(t)

	iter, err := shakti.NewIterator(nil, nil)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	// partially iterate through the memtable, then force a replace then fully iterate again. and repeat

	ks := 0
	for i := 0; i < 10; i++ {
		writeKVs(t, shakti, ks, 10)

		// Only iterate through some of them
		iteratePairs(t, iter, ks, 5)
		requireIterValid(t, iter, true)

		err = shakti.forceReplaceMemtable()
		require.NoError(t, err)

		ks += 5

		// Then iterate through the rest
		err = iter.Next()
		require.NoError(t, err)
		iteratePairs(t, iter, ks, 5)
		err = iter.Next()
		require.NoError(t, err)
		requireIterValid(t, iter, false)

		ks += 5
	}
}

func TestIterateThenReplaceThenAddWithSmallerKey(t *testing.T) {
	shakti := setupShakti(t)

	iter, err := shakti.NewIterator(nil, nil)
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	writeKVs(t, shakti, 0, 10)
	iteratePairs(t, iter, 0, 10)

	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)

	err = shakti.forceReplaceMemtable()
	require.NoError(t, err)

	// Now add some more but with smaller keys we've already seen, and different values
	writeKVsWithValueSuffix(t, shakti, 0, 10, "suf")

	// We shouldn't see these
	requireIterValid(t, iter, false)

	// Add some more - we should see these
	writeKVs(t, shakti, 10, 10)
	iteratePairs(t, iter, 10, 10)
	err = iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestIterateShuffledKeys(t *testing.T) {
	shakti := setupShakti(t)

	// Add a bunch of entries in random order, and add in small batches, replacing memtable each time
	// Then iterate through them and verify in order and all correct

	numEntries := 10000
	entries := prepareRanges(numEntries, 1, 0)
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })

	batchSize := 10
	var batch *mem.Batch
	for _, entry := range entries {
		if batch == nil {
			batch = &mem.Batch{KVs: map[string][]byte{}}
		}
		batch.KVs[string(entry.keyStart)] = entry.keyEnd
		if len(batch.KVs) == batchSize {
			err := shakti.Write(batch)
			require.NoError(t, err)
			batch = nil
			err = shakti.forceReplaceMemtable()
			require.NoError(t, err)
		}
	}

	iter, err := shakti.NewIterator(nil, nil)
	require.NoError(t, err)
	iteratePairs(t, iter, 0, numEntries)
}

func TestPeriodicMemtableReplace(t *testing.T) {
	// Make sure that the memtable is periodically replaced
	maxReplaceTime := 250 * time.Millisecond
	conf := cmn.Conf{
		MemtableMaxSizeBytes:      1024 * 1024,
		MemtableFlushQueueMaxSize: 4,
		TableFormat:               cmn.DataFormatV1,
		MemTableMaxReplaceTime:    maxReplaceTime,
	}
	shakti := setupShaktiWithConf(t, conf)
	cs := shakti.cloudStore.(*cloudstore.LocalStore) //nolint:forcetypeassert

	numIters := 3
	ks := 0
	start := time.Now()
	for i := 0; i < numIters; i++ {
		storeSize := cs.Size()

		writeKVs(t, shakti, ks, 10)

		commontest.WaitUntil(t, func() (bool, error) {
			// Wait until the SSTable should has been pushed to the cloud store
			return cs.Size() == storeSize+1, nil
		})

		// Make sure all the data is there
		iter, err := shakti.NewIterator(nil, nil)
		require.NoError(t, err)
		iteratePairs(t, iter, 0, 10*(i+1))
		err = iter.Next()
		require.NoError(t, err)
		requireIterValid(t, iter, false)
		ks += 10
	}
	dur := time.Now().Sub(start)
	require.True(t, dur > time.Duration(numIters)*maxReplaceTime)
}

func TestMemtableReplaceWhenMaxSizeReached(t *testing.T) {
	conf := cmn.Conf{
		MemtableMaxSizeBytes:      16 * 1024 * 1024,
		MemtableFlushQueueMaxSize: 10,
		TableFormat:               cmn.DataFormatV1,
		MemTableMaxReplaceTime:    30 * time.Second,
	}
	shakti := setupShaktiWithConf(t, conf)
	cs := shakti.cloudStore.(*cloudstore.LocalStore) //nolint:forcetypeassert

	// Add entries until several SSTables have been flushed to cloud store
	numSSTables := 10
	ks := 0
	sizeStart := 0
	for cs.Size() < sizeStart+numSSTables {
		writeKVs(t, shakti, ks, 10)
		ks += 10
	}

	//iter, err := shakti.NewIterator(nil, nil)
	//require.NoError(t, err)
	//
	//iteratePairs(t, iter, 0, ks)
}

func setupShakti(t *testing.T) *Shakti {
	t.Helper()
	conf := cmn.Conf{
		MemtableMaxSizeBytes:      1024 * 1024,
		MemtableFlushQueueMaxSize: 4,
		TableFormat:               cmn.DataFormatV1,
		MemTableMaxReplaceTime:    30 * time.Second,
	}
	return setupShaktiWithConf(t, conf)
}

func setupShaktiWithConf(t *testing.T, conf cmn.Conf) *Shakti {
	t.Helper()
	controllerConf := datacontroller.Conf{
		RegistryFormat:                 cmn.MetadataFormatV1,
		MasterRegistryRecordID:         "test.master",
		MaxRegistrySegmentTableEntries: 100,
		LogFileName:                    "shakti_repl.log",
	}
	cloudStore := cloudstore.NewLocalStore(100 * time.Millisecond)
	replicator := &cmn.NoopReplicator{}
	controller := datacontroller.NewController(controllerConf, cloudStore, replicator)
	err := controller.Start()
	require.NoError(t, err)
	shakti := NewShakti(1, cloudStore, controller, conf)
	err = shakti.Start()
	require.NoError(t, err)
	err = controller.SetLeader()
	require.NoError(t, err)
	return shakti
}

func iteratePairs(t *testing.T, iter iteration.Iterator, keyStart int, numPairs int) {
	t.Helper()
	for i := 0; i < numPairs; i++ {
		requireIterValid(t, iter, true)
		curr := iter.Current()
		//log.Printf("got key %s value %s", string(curr.Key), string(curr.Value))
		require.Equal(t, []byte(fmt.Sprintf("prefix/key-%010d", keyStart+i)), curr.Key)
		require.Equal(t, []byte(fmt.Sprintf("prefix/value-%010d", keyStart+i)), curr.Value)
		if i != numPairs-1 {
			err := iter.Next()
			require.NoError(t, err)
		}
	}
}

func requireIterValid(t *testing.T, iter iteration.Iterator, valid bool) {
	t.Helper()
	v, err := iter.IsValid()
	require.NoError(t, err)
	require.Equal(t, valid, v)
}

func writeKVs(t *testing.T, shakti *Shakti, keyStart int, numPairs int) { //nolint:unparam
	t.Helper()
	writeKVsWithParams(t, shakti, keyStart, numPairs, "", false)
}

func writeKVsWithTombstones(t *testing.T, shakti *Shakti, keyStart int, numPairs int) {
	t.Helper()
	writeKVsWithParams(t, shakti, keyStart, numPairs, "", true)
}

func writeKVsWithValueSuffix(t *testing.T, shakti *Shakti, keyStart int, numPairs int, valueSuffix string) {
	t.Helper()
	writeKVsWithParams(t, shakti, keyStart, numPairs, valueSuffix, false)
}

func writeKVsWithParams(t *testing.T, shakti *Shakti, keyStart int, numPairs int, valueSuffix string, tombstones bool) {
	t.Helper()
	kvs := make(map[string][]byte, numPairs)
	for i := 0; i < numPairs; i++ {
		k := fmt.Sprintf("prefix/key-%010d", keyStart+i)
		var v []byte
		if tombstones {
			v = nil
		} else {
			v = []byte(fmt.Sprintf("prefix/value-%010d%s", keyStart+i, valueSuffix))
		}
		kvs[k] = v
	}
	batch := &mem.Batch{
		KVs: kvs,
	}
	err := shakti.Write(batch)
	require.NoError(t, err)
}

func prepareRanges(numEntries int, rangeSize int, rangeGap int) []rng {
	entries := make([]rng, 0, numEntries)
	start := 0
	for i := 0; i < numEntries; i++ {
		ks := fmt.Sprintf("prefix/key-%010d", i)
		ke := fmt.Sprintf("prefix/value-%010d", i)
		start += rangeSize + rangeGap
		entries = append(entries, rng{
			keyStart: []byte(ks),
			keyEnd:   []byte(ke),
		})
	}
	return entries
}

type rng struct {
	keyStart []byte
	keyEnd   []byte
}
