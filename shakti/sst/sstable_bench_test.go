package sst

import (
	"fmt"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/stretchr/testify/require"
	"testing"
)

func BenchmarkBuildSSTable(b *testing.B) {
	commonPrefix := []byte("keyprefix/")
	// This gives SSTable size of approx 10MB
	numEntries := 32000
	valuePrefixLength := 250
	var valuePrefix []byte
	for i := 0; i < valuePrefixLength; i++ {
		valuePrefix = append(valuePrefix, byte(i))
	}

	// Build once outside the timer to get the size
	iter := prepareInput(commonPrefix, valuePrefix, numEntries)
	sstable, _, _, err := BuildSSTable(cmn.DataFormatV1, 0, numEntries, commonPrefix, iter)
	require.NoError(b, err)
	bufferSize := len(sstable.Serialize())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		iter := prepareInput(commonPrefix, valuePrefix, numEntries)
		b.StartTimer()
		_, _, _, err := BuildSSTable(cmn.DataFormatV1, numEntries, bufferSize, commonPrefix, iter)
		require.NoError(b, err)
	}
}

func BenchmarkSerializeSSTable(b *testing.B) {
	commonPrefix := []byte("keyprefix/")
	// This gives SSTable size of approx 10MB
	numEntries := 32000
	valuePrefixLength := 250
	var valuePrefix []byte
	for i := 0; i < valuePrefixLength; i++ {
		valuePrefix = append(valuePrefix, byte(i))
	}

	// Build once outside the timer to get the size
	iter := prepareInput(commonPrefix, valuePrefix, numEntries)
	sstable, _, _, err := BuildSSTable(cmn.DataFormatV1, 0, numEntries, commonPrefix, iter)
	require.NoError(b, err)
	bufferSize := len(sstable.Serialize())
	sstable, _, _, err = BuildSSTable(cmn.DataFormatV1, bufferSize, numEntries, commonPrefix, iter)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes := sstable.Serialize()
		require.NotNil(b, bytes)
	}
}

func BenchmarkSeekSSTable(b *testing.B) {
	commonPrefix := []byte("keyprefix/")
	// This gives SSTable size of approx 10MB
	numEntries := 32000
	valuePrefixLength := 250
	var valuePrefix []byte
	for i := 0; i < valuePrefixLength; i++ {
		valuePrefix = append(valuePrefix, byte(i))
	}
	iter := prepareInput(commonPrefix, valuePrefix, numEntries)
	sstable, _, _, err := BuildSSTable(cmn.DataFormatV1, 0, 0, commonPrefix, iter)
	require.NoError(b, err)
	keysToSeek := make([][]byte, numEntries)
	for i := 0; i < numEntries; i++ {
		keysToSeek[i] = []byte(fmt.Sprintf("%ssomekey-%010d", string(commonPrefix), i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// NOTE!! We seek all the keys, so the average time for a seek will be the reported time / numEntries
		for j := 0; j < numEntries; j++ {
			keyToSeek := keysToSeek[j]
			iter, err := sstable.NewIterator(keyToSeek, nil)
			require.NoError(b, err)
			requireIterValid(b, iter, true)
		}
	}
}

func BenchmarkIterateAllSSTable(b *testing.B) {
	commonPrefix := []byte("keyprefix/")
	// This gives SSTable size of approx 10MB
	numEntries := 32000
	valuePrefixLength := 250
	var valuePrefix []byte
	for i := 0; i < valuePrefixLength; i++ {
		valuePrefix = append(valuePrefix, byte(i))
	}
	iter := prepareInput(commonPrefix, valuePrefix, numEntries)
	sstable, _, _, err := BuildSSTable(cmn.DataFormatV1, 0, 0, commonPrefix, iter)
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, err := sstable.NewIterator(commonPrefix, nil)
		if err != nil {
			panic(err)
		}
		count := 0
		for {
			v, err := iter.IsValid()
			if err != nil {
				panic(err)
			}
			if !v {
				break
			}
			curr := iter.Current()
			if curr.Key == nil {
				panic("nil key")
			}
			if curr.Value == nil {
				panic("nil value")
			}
			err = iter.Next()
			if err != nil {
				panic(err)
			}
			count++
		}
		if numEntries != count {
			panic("wrong number of entries")
		}
	}
}
