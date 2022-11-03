package nexus

import (
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/iteration"
	"github.com/squareup/pranadb/shakti/sst"
)

// MergeSSTables takes a list of SSTables, and merges them to produce one or more output SSTables
// Tables lower in the list take precedence to tables higher in the list when a common key is found

const targetTableSizeBytes = 10 * 1024 * 1024

type TableEntry struct {
	sstable     *sst.SSTable
	smallestKey []byte
	largestKey  []byte
}

func MergeSSTables(format cmn.DataFormat, ssTables ...*sst.SSTable) ([]TableEntry, error) {
	iters := make([]iteration.Iterator, len(ssTables))
	totEntries := 0
	var commonPrefix []byte // estimate of common prefix
	for i, sstable := range ssTables {
		var err error
		iters[i], err = sstable.NewIterator(nil, nil)
		if err != nil {
			return nil, err
		}
		totEntries += sstable.NumEntries()
		if commonPrefix == nil {
			commonPrefix = sstable.CommonPrefix()
		} else {
			commonPrefix = findCommonPrefix(commonPrefix, sstable.CommonPrefix())
		}
	}
	mi, err := iteration.NewMergingIterator(iters, true)
	if err != nil {
		return nil, err
	}
	var outTables []TableEntry
	for mi.IsValid() {
		msIter := newMaxSizeIterator(mi, targetTableSizeBytes, len(commonPrefix))
		// TODO come up with a good estimate of the below!
		buffSizeEstimate := 0
		entriesEstimate := 0
		ssttable, smallestKey, largestKey, err := sst.BuildSSTable(format, buffSizeEstimate, entriesEstimate, commonPrefix, msIter)
		if err != nil {
			return nil, err
		}
		outTables = append(outTables, TableEntry{
			sstable:     ssttable,
			smallestKey: smallestKey,
			largestKey:  largestKey,
		})
	}

	return outTables, nil
}

func newMaxSizeIterator(mergeIterator iteration.Iterator, maxTableSize int, commonPrefixLenEstimate int) iteration.Iterator {
	return &maxSizeIterator{
		mergIter:                mergeIterator,
		maxTableDataSize:        maxTableSize,
		commonPrefixLenEstimate: commonPrefixLenEstimate,
		valid:                   true,
	}
}

type maxSizeIterator struct {
	mergIter                iteration.Iterator
	tableDataSizeEstimate   int
	maxTableDataSize        int
	commonPrefixLenEstimate int
	valid                   bool
}

func (s *maxSizeIterator) Current() cmn.KV {
	curr := s.mergIter.Current()
	s.tableDataSizeEstimate += len(curr.Key) + len(curr.Value) - s.commonPrefixLenEstimate + 8 // Doesn't include index or metadata
	return curr
}

func (s *maxSizeIterator) Next() error {
	if s.tableDataSizeEstimate >= s.maxTableDataSize {
		s.valid = false
		return nil
	}
	return s.Next()
}

func (s *maxSizeIterator) IsValid() bool {
	if !s.valid {
		return false
	}
	return s.mergIter.IsValid()
}

func findCommonPrefix(pref1 []byte, pref2 []byte) []byte {
	// TODO
	return nil
}
