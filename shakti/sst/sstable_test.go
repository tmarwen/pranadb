package sst

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/iteration"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestBuildTable(t *testing.T) {
	commonPrefix := []byte("keyprefix/")
	numEntries := 1000
	iter := prepareInput(commonPrefix, []byte("valueprefix/"), numEntries)
	sstable, smallestKey, largestKey, err := BuildSSTable(cmn.DataFormatV1, 0, 0, commonPrefix, iter)
	require.NoError(t, err)
	require.Equal(t, cmn.DataFormatV1, sstable.format)
	require.Equal(t, numEntries, sstable.numEntries)
	require.Equal(t, commonPrefix, sstable.commonPrefix)
	expectedSmallestKey := []byte(fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), 0))
	expectedLargestKey := []byte(fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), numEntries-1))
	require.Equal(t, expectedSmallestKey, smallestKey)
	require.Equal(t, expectedLargestKey, largestKey)
}

func TestBuildWithTombstones(t *testing.T) {
	commonPrefix := []byte("keyPrefix/")

	gi := &iteration.StaticIterator{}
	gi.AddKV([]byte("keyPrefix/key0"), nil)
	gi.AddKV([]byte("keyPrefix/key1"), []byte("val1"))
	gi.AddKV([]byte("keyPrefix/key2"), []byte("val2"))
	gi.AddKV([]byte("keyPrefix/key3"), nil)

	sstable, _, _, err := BuildSSTable(cmn.DataFormatV1, 0, 0, commonPrefix, gi)
	require.NoError(t, err)

	iter, err := sstable.NewIterator([]byte("keyPrefix/"), nil)
	require.NoError(t, err)

	require.True(t, iter.IsValid())
	curr := iter.Current()
	require.Equal(t, "keyPrefix/key0", string(curr.Key))
	require.Nil(t, curr.Value)
	err = iter.Next()
	require.NoError(t, err)

	require.True(t, iter.IsValid())
	curr = iter.Current()
	require.Equal(t, "keyPrefix/key1", string(curr.Key))
	require.Equal(t, "val1", string(curr.Value))
	err = iter.Next()
	require.NoError(t, err)

	require.True(t, iter.IsValid())
	curr = iter.Current()
	require.Equal(t, "keyPrefix/key2", string(curr.Key))
	require.Equal(t, "val2", string(curr.Value))
	err = iter.Next()
	require.NoError(t, err)

	require.True(t, iter.IsValid())
	curr = iter.Current()
	require.Equal(t, "keyPrefix/key3", string(curr.Key))
	require.Nil(t, curr.Value)
	err = iter.Next()
	require.NoError(t, err)

	require.False(t, iter.IsValid())
}

func TestSeek(t *testing.T) {
	commonPrefix := []byte("keyprefix/")
	numEntries := 1000

	iter := prepareInput(commonPrefix, []byte("valueprefix/"), numEntries)

	// Add a few more entries so we can test seeking to next
	key := fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), 1500)
	value := fmt.Sprintf("%ssomevalue-%010d", "valueprefix/", 1500)
	iter.AddKVAsString(key, value)
	key = fmt.Sprintf("%ssomekey-%010d1234", string(commonPrefix), 1550)
	value = fmt.Sprintf("%ssomevalue-%010d1234", "valueprefix/", 1550)
	iter.AddKVAsString(key, value)
	key = fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), 1600)
	value = fmt.Sprintf("%ssomevalue-%010d", "valueprefix/", 1600)
	iter.AddKVAsString(key, value)

	sstable, _, _, err := BuildSSTable(cmn.DataFormatV1, 0, 0, commonPrefix, iter)
	require.NoError(t, err)

	// Seek some random keys - exact match
	for i := 0; i < 100; i++ {
		r := rand.Intn(numEntries)
		k := []byte(fmt.Sprintf("keyprefix/somekey-%010d", r))
		v := []byte(fmt.Sprintf("valueprefix/somevalue-%010d", r))
		seek(t, k, k, v, true, sstable)
	}

	//boundary cases
	seek(t, []byte("keyprefix/somekey-0000000000"), []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	k := []byte(fmt.Sprintf("keyprefix/somekey-%010d", numEntries-1))
	v := []byte(fmt.Sprintf("valueprefix/somevalue-%010d", numEntries-1))
	seek(t, k, k, v, true, sstable)

	//not found - as keys all greater than keys in sstable
	seek(t, []byte("keyprefix/t"), nil, nil, false, sstable)
	seek(t, []byte("keyprefix/somekey-0000002000"), nil, nil, false, sstable)
	seek(t, []byte("keyprefix/uqwdiquwhdiuqwhdiuqhwdiuqhwdiuhqwd"), nil, nil, false, sstable)

	//should find next key greater than
	seek(t, []byte("keyprefix/"), []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	seek(t, []byte("keyprefix/r"), []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	seek(t, []byte("keyprefix/somekey"), []byte("keyprefix/somekey-0000000000"), []byte("valueprefix/somevalue-0000000000"), true, sstable)
	seek(t, []byte("keyprefix/somekey-00000005001"), []byte("keyprefix/somekey-0000000501"), []byte("valueprefix/somevalue-0000000501"), true, sstable)

	seek(t, []byte("keyprefix/somekey-0000001450"), []byte("keyprefix/somekey-0000001500"), []byte("valueprefix/somevalue-0000001500"), true, sstable)
	seek(t, []byte("keyprefix/somekey-0000001450999"), []byte("keyprefix/somekey-0000001500"), []byte("valueprefix/somevalue-0000001500"), true, sstable)
	seek(t, []byte("keyprefix/somekey-0000001549"), []byte("keyprefix/somekey-00000015501234"), []byte("valueprefix/somevalue-00000015501234"), true, sstable)
	seek(t, []byte("keyprefix/somekey-000000154999"), []byte("keyprefix/somekey-00000015501234"), []byte("valueprefix/somevalue-00000015501234"), true, sstable)
	seek(t, []byte("keyprefix/somekey-0000001550"), []byte("keyprefix/somekey-00000015501234"), []byte("valueprefix/somevalue-00000015501234"), true, sstable)
	seek(t, []byte("keyprefix/somekey-0000001599"), []byte("keyprefix/somekey-0000001600"), []byte("valueprefix/somevalue-0000001600"), true, sstable)
	seek(t, []byte("keyprefix/somekey-000000159999"), []byte("keyprefix/somekey-0000001600"), []byte("valueprefix/somevalue-0000001600"), true, sstable)
	seek(t, []byte("keyprefix/somekey-0000001600"), []byte("keyprefix/somekey-0000001600"), []byte("valueprefix/somevalue-0000001600"), true, sstable)
}

func seek(t *testing.T, seekKey []byte, expectedKey []byte, expectedValue []byte, valid bool, sstable *SSTable) {
	t.Helper()
	log.Printf("Seeking to key %s", string(seekKey))
	iter, err := sstable.NewIterator(seekKey, nil)
	require.NoError(t, err)
	if !valid {
		require.False(t, iter.IsValid())
		return
	}
	require.True(t, iter.IsValid())
	kv := iter.Current()
	require.Equal(t, string(expectedKey), string(kv.Key))
	require.Equal(t, string(expectedValue), string(kv.Value))
}

func TestIterateWithGaps(t *testing.T) {
	commonPrefix := []byte("keyprefix/")
	it := &iteration.StaticIterator{}
	// Add a few more entries so we can test seeking to next
	key := fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), 1500)
	value := fmt.Sprintf("%ssomevalue-%010d", "valueprefix/", 1500)
	it.AddKVAsString(key, value)
	key = fmt.Sprintf("%ssomekey-%010d1234", string(commonPrefix), 1550)
	value = fmt.Sprintf("%ssomevalue-%010d1234", "valueprefix/", 1550)
	it.AddKVAsString(key, value)
	key = fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), 1600)
	value = fmt.Sprintf("%ssomevalue-%010d", "valueprefix/", 1600)
	it.AddKVAsString(key, value)

	sstable, _, _, err := BuildSSTable(cmn.DataFormatV1, 0, 0, commonPrefix, it)
	require.NoError(t, err)
	iter, err := sstable.NewIterator([]byte("keyprefix/somekey-0000001501"), nil)
	require.NoError(t, err)
	require.True(t, iter.IsValid())
	kv := iter.Current()
	require.Equal(t, []byte("keyprefix/somekey-00000015501234"), kv.Key)
	require.Equal(t, []byte("valueprefix/somevalue-00000015501234"), kv.Value)
	err = iter.Next()
	require.NoError(t, err)
	require.True(t, iter.IsValid())
	kv = iter.Current()
	require.Equal(t, []byte("keyprefix/somekey-0000001600"), kv.Key)
	require.Equal(t, []byte("valueprefix/somevalue-0000001600"), kv.Value)
	err = iter.Next()
	require.NoError(t, err)
	require.False(t, iter.IsValid())
}

func TestIterate(t *testing.T) {
	commonPrefix := []byte("keyprefix/")
	testIterate(t, commonPrefix, nil, 0, 999)
	testIterate(t, commonPrefix, []byte("keyprefix/somekey-0000000450"), 0, 449)
	testIterate(t, []byte("keyprefix/somekey-0000000300"), nil, 300, 999)
	testIterate(t, []byte("keyprefix/somekey-0000000300999"), nil, 301, 999)
	testIterate(t, []byte("keyprefix/somekey-0000000300"), []byte("keyprefix/somekey-0000000900"), 300, 899)
	testIterate(t, []byte("keyprefix/somekey-0000000300"), []byte("keyprefix/somekey-0000000999"), 300, 998)
	testIterate(t, []byte("keyprefix/somekey-0000000300"), []byte("keyprefix/somekey-0000000999999"), 300, 999)
	testIterate(t, []byte("keyprefix/somekey-0000000300"), []byte("keyprefix/somekey-0000001000"), 300, 999)
	testIterate(t, []byte("keyprefix/somekey-0000000700"), []byte("keyprefix/somekey-0000000701"), 700, 700)
	testIterate(t, []byte("keyprefix/somekey-0000000700"), []byte("keyprefix/somekey-0000000700"), -1, -1)
	testIterate(t, []byte("keyprefix/somekey-0000001000"), []byte("keyprefix/somekey-0000001001"), -1, -1)
	testIterate(t, []byte("keyprefix/t"), []byte("keyprefix/u"), -1, -1)
}

func testIterate(t *testing.T, startKey []byte, endKey []byte, firstExpected int, lastExpected int) {
	t.Helper()
	commonPrefix := []byte("keyprefix/")
	numEntries := 1000
	it := prepareInput(commonPrefix, []byte("valueprefix/"), numEntries)
	sstable, _, _, err := BuildSSTable(cmn.DataFormatV1, 0, 0, commonPrefix, it)
	require.NoError(t, err)

	iter, err := sstable.NewIterator(startKey, endKey)
	require.NoError(t, err)

	if firstExpected == -1 {
		require.False(t, iter.IsValid())
		return
	}

	i := firstExpected
	for iter.IsValid() && i <= lastExpected {
		kv := iter.Current()
		k := []byte(fmt.Sprintf("keyprefix/somekey-%010d", i))
		v := []byte(fmt.Sprintf("valueprefix/somevalue-%010d", i))
		require.Equal(t, k, kv.Key)
		require.Equal(t, v, kv.Value)
		i++
		err := iter.Next()
		require.NoError(t, err)
	}
	require.False(t, iter.IsValid())
	require.Nil(t, iter.Next())
}

func TestSerializeDeserialize(t *testing.T) {
	commonPrefix := []byte("keyprefix/")
	numEntries := 1000
	iter := prepareInput(commonPrefix, []byte("valueprefix/"), numEntries)
	sstable, _, _, err := BuildSSTable(cmn.DataFormatV1, 0, 0, commonPrefix, iter)
	require.NoError(t, err)
	buff := sstable.Serialize()

	sstable2 := &SSTable{}
	sstable2.Deserialize(buff, 0)

	require.Equal(t, sstable.format, sstable2.format)
	require.Equal(t, sstable.indexOffset, sstable2.indexOffset)
	require.Equal(t, sstable.numEntries, sstable2.numEntries)
	require.Equal(t, sstable.maxKeyLength, sstable2.maxKeyLength)
	require.Equal(t, sstable.commonPrefix, sstable2.commonPrefix)
	require.Equal(t, sstable.data, sstable2.data)
}

func prepareInput(keyPrefix []byte, valuePrefix []byte, numEntries int) *iteration.StaticIterator {
	gi := &iteration.StaticIterator{}
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("%ssomekey-%010d", string(keyPrefix), i)
		value := fmt.Sprintf("%ssomevalue-%010d", string(valuePrefix), i)
		gi.AddKVAsString(key, value)
	}
	return gi
}
