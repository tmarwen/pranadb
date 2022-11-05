package shakti

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/shakti/cloudstore"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/iteration"
	"github.com/squareup/pranadb/shakti/mem"
	"github.com/squareup/pranadb/shakti/nexus"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

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

func setupShakti(t *testing.T) *Shakti {
	t.Helper()
	conf := cmn.Conf{
		MemtableMaxSizeBytes:      1024 * 1024,
		MemtableFlushQueueMaxSize: 4,
		TableFormat:               cmn.DataFormatV1,
	}
	controllerConf := nexus.Conf{
		RegistryFormat:                 cmn.MetadataFormatV1,
		MasterRegistryRecordID:         "test.master",
		MaxRegistrySegmentTableEntries: 100,
		LogFileName:                    "shakti_repl.log",
	}
	cloudStore := &cloudstore.LocalStore{}
	replicator := &cmn.NoopReplicator{}
	controller := nexus.NewController(controllerConf, cloudStore, replicator)
	err := controller.Start()
	require.NoError(t, err)
	shakti := NewShakti(cloudStore, controller, conf)
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
		log.Printf("got key %s value %s", string(curr.Key), string(curr.Value))
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
		log.Printf("writing key %s value %s", k, string(v))
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
