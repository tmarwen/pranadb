package shakti

import (
	"bytes"
	"github.com/andy-kimball/arenaskl"
	"github.com/squareup/pranadb/common"
)

type DeleteRange struct {
	StartKey []byte
	EndKey []byte
}

func NewBatch() *Batch {
	return &Batch{
		KVs:          map[string][]byte{},
	}
}

type Batch struct {
	KVs map[string][]byte
	DeleteRanges []DeleteRange
}

type Memtable struct {
	sl *arenaskl.Skiplist
	maxSize int
	commonPrefix []byte
}

func NewMemtable(maxMemSize int) *Memtable {
	sl := arenaskl.NewSkiplist(arenaskl.NewArena(uint32(maxMemSize)))
	return &Memtable{
		sl: sl,
	}
}

func (m *Memtable) Write(batch *Batch) (bool, error) {

	// Before a batch is available for reading in the MemTable we must replicate it
	if err := m.replicateBatch(batch); err != nil {
		return false, err
	}

	// TODO experiment to see if perf difference by reusing iter
	var it arenaskl.Iterator
	it.Init(m.sl)

	for k, v := range batch.KVs {
		kk := common.StringToByteSliceZeroCopy(k)
		m.updateCommonPrefix(kk)
		if err := it.Add(kk, v, 0); err != nil {
			if err == arenaskl.ErrRecordExists {
				if err := it.Set(v, 0); err != nil {
					return false, err
				}
			} else if err == arenaskl.ErrArenaFull {
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
	// if it has recently joined, so it asks all nodes for current memtable state for same epoch. Nodes won't respond unless
	// they have seen one flush. Once it has received recovered data it can flush that and store it in S3.
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
	return &MemtableIterator{
		it: &it,
		keyStart: keyStart,
		keyEnd: keyEnd,
		valid: it.Valid(),
	}
}

func (m *Memtable) CommonPrefix() []byte {
	return m.commonPrefix
}

type MemtableIterator struct {
	it *arenaskl.Iterator
	keyStart []byte
	keyEnd []byte
	valid bool
}

func (m *MemtableIterator) Current() KV {
	if !m.valid {
		panic("not valid")
	}
	k := m.it.Key()
	v := m.it.Value()
	return KV{
		Key:   k,
		Value: v,
	}
}

func (m *MemtableIterator) Next() error {
	if !m.valid {
		panic("not valid")
	}
	m.it.Next()
	if !m.it.Valid() || (m.keyEnd != nil && bytes.Compare(m.it.Key(), m.keyEnd) >= 0) {
		// No more entries or end of range
		m.valid = false
	}
	return nil
}

func (m *MemtableIterator) IsValid() bool {
	return m.valid
}

