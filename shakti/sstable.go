package shakti

import (
	"bytes"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"math"
)

type KV struct {
	Key   []byte
	Value []byte
}

type SSTableID []byte

type Format byte

const (
	FormatV1 Format = 1
)

type SSTable struct {
	format       Format
	commonPrefix []byte
	maxKeyLength int
	numEntries   int
	indexOffset  int
	data         []byte
}

func BuildSSTable(format Format, buffSizeEstimate int, entriesEstimate int, commonPrefix []byte, iter Iterator) (*SSTable, []byte, []byte, error) {
	lcp := len(commonPrefix)

	// TODO 1. if stable has fewer than a few (say 5?) records then index can probably be ommitted
	// 2. Only index every N entries, not all of them

	type indexEntry struct {
		key    []byte
		offset uint32
	}

	var smallestKey, largestKey []byte

	indexEntries := make([]indexEntry, 0, entriesEstimate)
	buff := make([]byte, 0, buffSizeEstimate)

	// First byte is the format, then 4 bytes (uint32) which is an offset to the metadata section that we will fill in
	// later
	buff = append(buff, byte(format), 0, 0, 0, 0)

	maxKeyLengthWithoutPrefix := 0
	numEntries := 0
	first := true
	var prevKey []byte
	for iter.IsValid() {
		kv := iter.Current()
		// Sanity checks - can maybe remove them or activate them only with a flag for performance
		if len(kv.Key) < lcp || bytes.Compare(commonPrefix, kv.Key[:lcp]) != 0 {
			panic("key does not have common prefix")
		}
		if prevKey != nil && bytes.Compare(prevKey, kv.Key) >= 0 {
			panic("keys not in order / contains duplicates")
		}
		prevKey = kv.Key
		if first {
			smallestKey = kv.Key
			first = false
		}
		offset := uint32(len(buff))
		keyNoPrefix := kv.Key[lcp:]
		lknp := len(keyNoPrefix)
		if lknp > maxKeyLengthWithoutPrefix {
			maxKeyLengthWithoutPrefix = lknp
		}
		buff = appendBytesWithLengthPrefix(buff, keyNoPrefix)
		buff = appendBytesWithLengthPrefix(buff, kv.Value)
		indexEntries = append(indexEntries, indexEntry{
			key:    keyNoPrefix,
			offset: offset,
		})
		numEntries++
		largestKey = kv.Key

		if err := iter.Next(); err != nil {
			return nil, nil, nil, err
		}
	}

	indexOffset := len(buff)

	for _, entry := range indexEntries {
		buff = append(buff, entry.key...)
		paddingBytes := maxKeyLengthWithoutPrefix - len(entry.key)
		// TODO quicker way?
		for j := 0; j < paddingBytes; j++ {
			buff = append(buff, 0)
		}
		buff = common.AppendUint32ToBufferLE(buff, entry.offset)
	}

	// Now fill in metadata offset
	metadataOffset := len(buff)
	if metadataOffset > math.MaxUint32 {
		return nil, nil, nil, errors.New("SSTable too big")
	}
	buff[1] = byte(metadataOffset)
	buff[2] = byte(metadataOffset >> 8)
	buff[3] = byte(metadataOffset >> 16)
	buff[4] = byte(metadataOffset >> 24)

	return &SSTable{
		format:       format,
		commonPrefix: commonPrefix,
		maxKeyLength: maxKeyLengthWithoutPrefix,
		numEntries:   numEntries,
		indexOffset:  indexOffset,
		data:         buff,
	}, smallestKey, largestKey, nil
}

func (s *SSTable) Serialize() []byte {
	// To avoid copying the data buffer, we put all the meta-data at the end
	buff := common.AppendUint32ToBufferLE(s.data, uint32(len(s.commonPrefix)))
	buff = append(buff, s.commonPrefix...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(s.maxKeyLength))
	buff = common.AppendUint32ToBufferLE(buff, uint32(s.numEntries))
	buff = common.AppendUint32ToBufferLE(buff, uint32(s.indexOffset))
	return buff
}

func (s *SSTable) Deserialize(buff []byte, offset int) int {
	s.format = Format(buff[offset])
	offset++
	var metadataOffset uint32
	metadataOffset, offset = common.ReadUint32FromBufferLE(buff, offset)
	var lcp uint32
	lcp, offset = common.ReadUint32FromBufferLE(buff, int(metadataOffset))
	s.commonPrefix = buff[offset : offset+int(lcp)]
	offset += int(lcp)
	var mkl uint32
	mkl, offset = common.ReadUint32FromBufferLE(buff, offset)
	s.maxKeyLength = int(mkl)
	var ne uint32
	ne, offset = common.ReadUint32FromBufferLE(buff, offset)
	s.numEntries = int(ne)
	var io uint32
	io, offset = common.ReadUint32FromBufferLE(buff, offset)
	s.indexOffset = int(io)
	metadataLength := 4 + len(s.commonPrefix) + 12
	s.data = buff[:len(buff)-metadataLength]
	return offset
}

func (s *SSTable) CommonPrefix() []byte {
	return s.commonPrefix
}

func appendBytesWithLengthPrefix(buff []byte, bytes []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(bytes)))
	buff = append(buff, bytes...)
	return buff
}

func (s *SSTable) findOffset(key []byte) int {
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
		middle := low + (high-low)/2
		recordStart := middle*indexRecordLen + s.indexOffset
		midKey := s.data[recordStart : recordStart+s.maxKeyLength]
		if bytes.Compare(midKey, keyNoPrefix) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if high == outerHighBound {
		recordStart := high*indexRecordLen + s.indexOffset
		highKey := s.data[recordStart : recordStart+s.maxKeyLength]
		if bytes.Compare(highKey, keyNoPrefix) < 0 {
			// Didn't find writeIter
			return -1
		}
	}
	recordStart := high*indexRecordLen + s.indexOffset
	valueStart := recordStart + s.maxKeyLength
	off, _ := common.ReadUint32FromBufferLE(s.data, valueStart)
	return int(off)
}

func (s *SSTable) NewIterator(keyStart []byte, keyEnd []byte) (Iterator, error) {
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
	currkV     KV
	keyEnd     []byte
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
	tableCache *TableCache
	keyStart   []byte
	keyEnd     []byte
	iter       Iterator
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
