package shakti

import (
	"bytes"
	"github.com/andy-kimball/arenaskl"
	"github.com/squareup/pranadb/common"
	"sync/atomic"
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
	KVs          map[string][]byte
	DeleteRanges []DeleteRange
}

type Memtable struct {
	sl           *arenaskl.Skiplist
	maxSize      int
	commonPrefix []byte
	sstableInfo  atomic.Value
	writeIter    arenaskl.Iterator
}

type SSTableInfo struct {
	ssTableID   SSTableID
	largestKey  []byte
	smallestKey []byte
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

	// Before a batch is available for reading in the MemTable we must replicate writeIter
	if err := m.replicateBatch(batch); err != nil {
		return false, err
	}

	for k, v := range batch.KVs {
		kk := common.StringToByteSliceZeroCopy(k)
		m.updateCommonPrefix(kk)
		var err error
		if err = m.writeIter.Add(kk, v, 0); err != nil {
			if err == arenaskl.ErrRecordExists {
				err = m.writeIter.Set(v, 0)
			}
		}
		if err != nil {
			if err == arenaskl.ErrArenaFull {
				// Memtable has reached max size
				return false, nil
			} else {
				return false, err
			}
		}
	}

	return true, nil
}

func (m *Memtable) updateCommonPrefix(key []byte) {
	lk := len(key)
	lcp := len(m.commonPrefix)
	var l int
	if lk < lcp {
		l = lk
	} else {
		l = lcp
	}
	i := 0
	for i = 0; i < l; i++ {
		if key[i] != m.commonPrefix[i] {
			break
		}
	}
	if i != lcp {
		m.commonPrefix = m.commonPrefix[:i]
	}
}

func (m *Memtable) replicateBatch(batch *Batch) error {
	// We replicate batches to all nodes, we also replicate an epoch number in the batch.
	// Each replica maintains a copy of the memtable but only the processor actually stores SSTable to cloud
	// We also replicate a notification when the memtable is flushed.
	// Replicas only apply replicated batches if they have seen at least one flush or state won't be same.
	// On failure of processor, another replica is elected processor. It may not have all data since last flush, especially
	// if writeIter has recently joined, so writeIter asks all nodes for current memtable state for same epoch. Nodes won't respond unless
	// they have seen one flush. Once writeIter has received recovered data writeIter can flush that and store writeIter in S3.
	//

	return nil
}

func (m *Memtable) NewIterator(keyStart []byte, keyEnd []byte) Iterator {
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

func (m *MemtableIterator) Current() KV {
	if !m.it.Valid() {
		panic("not valid")
	}
	k := m.it.Key()
	v := m.it.Value()
	if len(v) == 0 {
		v = nil
	}
	return KV{
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

func (m *MemtableIterator) IsValid() bool {
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
		return false
	}
	if m.it.Valid() {
		return true
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
			return true
		} else {
			m.prevIt = &cp
		}
	}
	return false
}

// Called after the ssTable for the memtable has been stored to cloud storage
func (m *Memtable) setSSTableInfo(ssTableInfo *SSTableInfo) {
	m.sstableInfo.Store(ssTableInfo)
}

func (m *Memtable) getSSTableInfo() *SSTableInfo {
	s := m.sstableInfo.Load()
	if s == nil {
		return nil
	}
	return s.(*SSTableInfo)
}
