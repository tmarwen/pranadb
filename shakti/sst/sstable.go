package sst

import (
	"bytes"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/iteration"
	"math"
)

type SSTableID []byte

type SSTable struct {
	format       cmn.DataFormat
	commonPrefix []byte
	maxKeyLength int
	numEntries   int
	indexOffset  int
	data         []byte
}

func BuildSSTable(format cmn.DataFormat, buffSizeEstimate int, entriesEstimate int, commonPrefix []byte, iter iteration.Iterator) (*SSTable, []byte, []byte, error) {
	lcp := len(commonPrefix)

	// TODO 1. if stable has fewer than a few (say 5?) records then index can probably be omitted
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
	for {
		v, err := iter.IsValid()
		if err != nil {
			return nil, nil, nil, err
		}
		if !v {
			break
		}
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
	s.format = cmn.DataFormat(buff[offset])
	offset++
	var metadataOffset uint32
	metadataOffset, _ = common.ReadUint32FromBufferLE(buff, offset)
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

func (s *SSTable) SizeBytes() int {
	return len(s.data) + 16 + len(s.commonPrefix)
}

func (s *SSTable) CommonPrefix() []byte {
	return s.commonPrefix
}

func (s *SSTable) NumEntries() int {
	return s.numEntries
}

func appendBytesWithLengthPrefix(buff []byte, bytes []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(bytes)))
	buff = append(buff, bytes...)
	return buff
}

func (s *SSTable) findOffset(key []byte) int {
	indexRecordLen := s.maxKeyLength + 4
	d := bytes.Compare(key, s.commonPrefix)
	var index int
	if d <= 0 {
		index = 0
	} else {
		lcp := len(s.commonPrefix)
		keyNoPrefix := key[lcp:]
		// We do a binary search in the index
		low := 0
		outerHighBound := s.numEntries - 1
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
		index = high
	}

	recordStart := index*indexRecordLen + s.indexOffset
	valueStart := recordStart + s.maxKeyLength
	off, _ := common.ReadUint32FromBufferLE(s.data, valueStart)
	return int(off)
}
