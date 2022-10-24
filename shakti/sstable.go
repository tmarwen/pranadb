package shakti

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
)

type KV struct {
	Key []byte
	Value []byte
}

type SSTableID []byte

type SSTable struct {
	commonPrefix []byte
	maxKeyLength int
	numEntries int
	dataBuff []byte
	indexBuff []byte
}

func BuildSSTable(commonPrefix []byte, iter Iterator) (*SSTable, []byte, []byte, error) {
	lcp := len(commonPrefix)

	type indexEntry struct {
		key []byte
		offset uint32
	}

	var smallestKey, largestKey []byte

	// TODO better initial size estimates for index entries and databuff
	var indexEntries []indexEntry
	var dataBuff []byte
	maxKeyLength := 0
	numEntries := 0
	first := true
	for iter.IsValid() {
		kv := iter.Current()
		if len(kv.Key) < lcp || bytes.Compare(commonPrefix, kv.Key[:lcp]) != 0 {
			panic("key does not have common prefix")
		}
		if first {
			smallestKey = kv.Key
			first = false
		}
		log.Printf("adding key %s to sstable", string(kv.Key))
		offset := uint32(len(dataBuff))
		keyNoPrefix := kv.Key[lcp:]
		dataBuff = appendBytesWithLengthPrefix(dataBuff, keyNoPrefix)
		dataBuff = appendBytesWithLengthPrefix(dataBuff, kv.Value)
		indexEntries = append(indexEntries, indexEntry{
			key:    keyNoPrefix,
			offset: offset,
		})
		lknp := len(keyNoPrefix)
		if lknp > maxKeyLength {
			maxKeyLength = lknp
		}
		numEntries++
		largestKey = kv.Key
		if err := iter.Next(); err != nil {
			return nil, nil, nil, err
		}
	}

	indexBuff := make([]byte, 0, len(indexEntries) * (maxKeyLength + 4))
	for _, entry := range indexEntries {
		indexBuff = append(indexBuff, entry.key...)
		paddingBytes := maxKeyLength - len(entry.key)
		// TODO quicker way?
		for j := 0; j < paddingBytes; j++ {
			indexBuff = append(indexBuff, 0)
		}
		indexBuff = common.AppendUint32ToBufferLE(indexBuff, entry.offset)
	}

	return &SSTable{
		commonPrefix: commonPrefix,
		maxKeyLength: maxKeyLength,
		numEntries:   numEntries,
		dataBuff:     dataBuff,
		indexBuff:    indexBuff,
	}, smallestKey, largestKey, nil
}

func (s *SSTable) Serialize(buff []byte) []byte {
	return nil
}

func (s *SSTable) Deserialize(buff []byte, offset int) int {
	return 0
}

func appendBytesWithLengthPrefix(buff []byte, bytes []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(bytes)))
	buff = append(buff, bytes...)
	return buff
}

func (s *SSTable) FindOffset(key []byte) int {
	lcp := len(s.commonPrefix)
	if len(key) < lcp || bytes.Compare(s.commonPrefix, key[:lcp]) != 0 {
		panic("key does not have common prefix")
	}
	keyNoPrefix := key[lcp:]
	indexRecordLen := s.maxKeyLength + 4
	// We do a binary search in the index

	low := 0
	outerHighBound := int(s.numEntries) - 1
	high := outerHighBound
	for low < high {
		middle := low + (high - low) / 2
		recordStart := middle * indexRecordLen
		midKey := s.indexBuff[recordStart: recordStart + s.maxKeyLength]
		if bytes.Compare(midKey, keyNoPrefix) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if high == outerHighBound {
		// Didn't find it
		return -1
	}
	recordStart := high * indexRecordLen
	valueStart := recordStart + s.maxKeyLength
	off, _ := common.ReadUint32FromBufferLE(s.indexBuff, valueStart)
	return int(off)
}

func (s *SSTable) NewIterator(keyStart []byte, keyEnd []byte) (Iterator, error) {
	offset := s.FindOffset(keyStart)
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
		keyEnd: keyEndNoPrefix,
	}

	if err := si.Next(); err != nil {
		return nil, err
	}
	return si, nil
}

type SSTableIterator struct {
	ss         *SSTable
	nextOffset int
	valid bool
	currkV     KV
	keyEnd []byte
}

func (si *SSTableIterator) Current() KV {
	return si.currkV
}

func (si *SSTableIterator) Next() error {
	if si.nextOffset == -1 {
		si.valid = false
		return nil
	}

	var kl, vl uint32
	kl, si.nextOffset = common.ReadUint32FromBufferLE(si.ss.dataBuff, si.nextOffset)
	k := si.ss.dataBuff[si.nextOffset :si.nextOffset+int(kl)]
	if si.keyEnd != nil && bytes.Compare(k, si.keyEnd) >= 0 {
		// End of range
		si.nextOffset = -1
		si.valid = false
	} else {
		si.currkV.Key = k
		si.nextOffset += int(kl)
		vl, si.nextOffset = common.ReadUint32FromBufferLE(si.ss.dataBuff, si.nextOffset)
		si.currkV.Value = si.ss.dataBuff[si.nextOffset : si.nextOffset+int(vl)]
		si.nextOffset += int(vl)
		if si.nextOffset >= len(si.ss.dataBuff) {
			// Reached end of SSTable
			si.nextOffset = -1
		}
		si.valid = true
	}

	return nil
}

func (si* SSTableIterator) IsValid() bool {
	return si.valid
}

type LazySSTableIterator struct {
	tableID SSTableID
	tableCache *TableCache
	keyStart []byte
	keyEnd []byte
	iter Iterator
}

func NewLazySSTableIterator(tableID SSTableID, tableCache *TableCache, keyStart []byte, keyEnd []byte) (Iterator, error) {
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

func (l *LazySSTableIterator) Current() KV {
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


