package mem

import (
	"bytes"
	"github.com/andy-kimball/arenaskl"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	common2 "github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/iteration"
)

type DeleteRange struct {
	StartKey []byte
	EndKey   []byte
}

func NewBatch() *Batch {
	return &Batch{
		KVs: map[string][]byte{},
	}
}

type Batch struct {
	// TODO we should use a skiplist not a map, as it's better if we apply entries to the memtable in key order
	// rather than undefined map order, as it's likely to be faster (benchmark this!)
	KVs          map[string][]byte
	DeleteRanges []DeleteRange
}

func (b *Batch) Serialize(buff []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(b.KVs)))
	for k, v := range b.KVs {
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(k)))
		buff = append(buff, common.StringToByteSliceZeroCopy(k)...)
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(v)))
		buff = append(buff, v...)
	}
	// TODO delete ranges
	return buff
}

func (b *Batch) Deserialize(buff []byte, offset int) int {
	l, offset := common.ReadUint32FromBufferLE(buff, offset)
	b.KVs = make(map[string][]byte, l)
	for i := 0; i < int(l); i++ {
		var lk uint32
		lk, offset = common.ReadUint32FromBufferLE(buff, offset)
		k := common.ByteSliceToStringZeroCopy(buff[offset : offset+int(lk)])
		offset += int(lk)
		var lv uint32
		lv, offset = common.ReadUint32FromBufferLE(buff, offset)
		v := buff[offset : offset+int(lv)]
		offset += int(lv)
		b.KVs[k] = v
	}
	// TODO delete ranges
	return offset
}

// Memtable TODO range deletes
type Memtable struct {
	sl           *arenaskl.Skiplist
	commonPrefix []byte
	writeIter    arenaskl.Iterator
}

func NewMemtable(maxMemSize int) *Memtable {
	sl := arenaskl.NewSkiplist(arenaskl.NewArena(uint32(maxMemSize)))
	mt := &Memtable{
		sl: sl,
	}
	mt.writeIter.Init(sl)
	return mt
}

func (m *Memtable) Write(batch *Batch) (bool, error) {
	for k, v := range batch.KVs {
		kk := common.StringToByteSliceZeroCopy(k)
		if m.commonPrefix == nil {
			m.commonPrefix = kk
		} else {
			commonPrefixLen := findCommonPrefix(kk, m.commonPrefix)
			if len(m.commonPrefix) != commonPrefixLen {
				m.commonPrefix = m.commonPrefix[:commonPrefixLen]
			}
		}
		var err error
		if err = m.writeIter.Add(kk, v, 0); err != nil {
			if err == arenaskl.ErrRecordExists {
				err = m.writeIter.Set(v, 0)
			}
		}
		if err != nil {
			if err == arenaskl.ErrArenaFull {
				log.Debug("memtable is full")
				// Memtable has reached max size
				return false, nil
			}
			return false, err
		}
		// We delete, as in the case the Arena becomes full, we want to retry the batch after the memtable is replaced
		// and we don't want to resubmit entries from the batch that were successfully submitted
		delete(batch.KVs, k)
	}
	return true, nil
}

func findCommonPrefix(key1 []byte, key2 []byte) int {
	lk1 := len(key1) //nolint:ifshort
	lk2 := len(key2) //nolint:ifshort
	var l int
	if lk1 < lk2 {
		l = lk1
	} else {
		l = lk2
	}
	var i int
	for i = 0; i < l; i++ {
		if key1[i] != key2[i] {
			break
		}
	}
	return i
}

func (m *Memtable) NewIterator(keyStart []byte, keyEnd []byte) iteration.Iterator {
	var it arenaskl.Iterator
	it.Init(m.sl)
	if keyStart == nil {
		it.SeekToFirst()
	} else {
		it.Seek(keyStart)
	}
	endOfRange := false
	if keyEnd != nil && it.Valid() && bytes.Compare(it.Key(), keyEnd) >= 0 {
		endOfRange = true
	}
	return &MemtableIterator{
		it:          &it,
		keyStart:    keyStart,
		keyEnd:      keyEnd,
		endOfRange:  endOfRange,
		initialSeek: it.Valid(),
	}
}

func (m *Memtable) CommonPrefix() []byte {
	return m.commonPrefix
}

type MemtableIterator struct {
	it          *arenaskl.Iterator
	prevIt      *arenaskl.Iterator
	keyStart    []byte
	keyEnd      []byte
	endOfRange  bool
	initialSeek bool
}

func (m *MemtableIterator) Current() common2.KV {
	if !m.it.Valid() {
		panic("not valid")
	}
	k := m.it.Key()
	v := m.it.Value()
	if len(v) == 0 {
		v = nil
	}
	return common2.KV{
		Key:   k,
		Value: v,
	}
}

func (m *MemtableIterator) Next() error {
	// we make a copy of the iter before advancing in case we advance off the end (invalid) and later
	// more records arrive
	prevCopy := *m.it
	m.it.Next()
	if m.keyEnd != nil && bytes.Compare(m.it.Key(), m.keyEnd) >= 0 {
		// end of range
		m.endOfRange = true
	}
	m.prevIt = &prevCopy
	return nil
}

func (m *MemtableIterator) IsValid() (bool, error) {
	if !m.initialSeek {
		if m.keyStart == nil {
			m.it.SeekToFirst()
		} else {
			m.it.Seek(m.keyStart)
		}
		if m.it.Valid() {
			m.initialSeek = true
		}
	}
	if m.endOfRange {
		return false, nil
	}
	if m.it.Valid() {
		return true, nil
	}
	// We have to cache the previous value of the iterator before we moved to nil node (invalid)
	// that's where new entries will be added
	if m.prevIt != nil {
		cp := *m.prevIt
		m.prevIt.Next()
		if m.prevIt.Valid() {
			// There are new entries - reset the iterator to prev.next
			m.it = m.prevIt
			m.prevIt = nil
			return true, nil
		}
		m.prevIt = &cp
	}
	return false, nil
}

func (m *MemtableIterator) Close() error {
	return nil
}
