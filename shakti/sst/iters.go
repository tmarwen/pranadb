package sst

import (
	"bytes"
	"github.com/squareup/pranadb/common"
	common2 "github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/iteration"
)

func (s *SSTable) NewIterator(keyStart []byte, keyEnd []byte) (iteration.Iterator, error) {
	offset := s.findOffset(keyStart)
	var keyEndNoPrefix []byte
	if keyEnd != nil {
		// TODO combine this duplicated code
		lcp := len(s.commonPrefix)
		if len(keyEnd) < lcp || bytes.Compare(s.commonPrefix, keyEnd[:lcp]) != 0 {
			panic("key does not have common prefix")
		}
		keyEndNoPrefix = keyEnd[lcp:]
	}
	si := &SSTableIterator{
		ss:         s,
		nextOffset: offset,
		keyEnd:     keyEndNoPrefix,
	}

	if err := si.Next(); err != nil {
		return nil, err
	}
	return si, nil
}

type SSTableIterator struct {
	ss         *SSTable
	nextOffset int
	valid      bool
	currkV     common2.KV
	keyEnd     []byte
}

func (si *SSTableIterator) Current() common2.KV {
	return si.currkV
}

func (si *SSTableIterator) Next() error {
	if si.nextOffset == -1 {
		si.valid = false
		return nil
	}

	var kl, vl uint32
	kl, si.nextOffset = common.ReadUint32FromBufferLE(si.ss.data, si.nextOffset)
	k := si.ss.data[si.nextOffset : si.nextOffset+int(kl)]
	if si.keyEnd != nil && bytes.Compare(k, si.keyEnd) >= 0 {
		// End of range
		si.nextOffset = -1
		si.valid = false
	} else {
		kb := make([]byte, 0, len(si.ss.commonPrefix)+len(k))
		kb = append(kb, si.ss.commonPrefix...)
		kb = append(kb, k...)
		si.currkV.Key = kb
		si.nextOffset += int(kl)
		vl, si.nextOffset = common.ReadUint32FromBufferLE(si.ss.data, si.nextOffset)
		if vl == 0 {
			si.currkV.Value = nil
		} else {
			si.currkV.Value = si.ss.data[si.nextOffset : si.nextOffset+int(vl)]
		}
		si.nextOffset += int(vl)
		if si.nextOffset >= si.ss.indexOffset { // Start of index data marks end of entries data
			// Reached end of SSTable
			si.nextOffset = -1
		}
		si.valid = true
	}

	return nil
}

func (si *SSTableIterator) IsValid() bool {
	return si.valid
}

type LazySSTableIterator struct {
	tableID    SSTableID
	tableCache *Cache
	keyStart   []byte
	keyEnd     []byte
	iter       iteration.Iterator
}

func NewLazySSTableIterator(tableID SSTableID, tableCache *Cache, keyStart []byte, keyEnd []byte) (iteration.Iterator, error) {
	it := &LazySSTableIterator{
		tableID:    tableID,
		tableCache: tableCache,
		keyStart:   keyStart,
		keyEnd:     keyEnd,
	}
	if err := it.Next(); err != nil {
		return nil, err
	}
	return it, nil
}

func (l *LazySSTableIterator) Current() common2.KV {
	return l.iter.Current()
}

func (l *LazySSTableIterator) Next() error {
	if l.iter == nil {
		ssTable, err := l.tableCache.GetSSTable(l.tableID)
		if err != nil {
			return err
		}
		l.iter, err = ssTable.NewIterator(l.keyEnd, l.keyEnd)
		if err != nil {
			return err
		}
	}
	return l.iter.Next()
}

func (l *LazySSTableIterator) IsValid() bool {
	return l.iter.IsValid()
}
