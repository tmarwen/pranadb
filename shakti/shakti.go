package shakti

import (
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"sync"
)

type Shakti struct {
	memtable *Memtable
	cloudStore CloudStore
	registry Registry
	TableCache *TableCache
	mtLock sync.RWMutex
	mtFlushChan chan struct{}
	mtFlushQueue []*Memtable
	// We use a separate lock to protect the flush queue as we don't want removing first element from queue to block
	// writes to the memtable
	mtFlushQueueLock common.SpinLock
}

type Iterator interface {
	Current() KV
	Next() error
	IsValid() bool
}

type Conf struct {
	MemtableMaxSizeBytes int
	MemtableFlushQueueMaxSize int
}

func NewShakti(cloudStore CloudStore, registry Registry, conf Conf) *Shakti {
	memtable := NewMemtable(conf.MemtableMaxSizeBytes )
	return &Shakti{
		memtable:   memtable,
		cloudStore: cloudStore,
		registry:   registry,
		TableCache: &TableCache{cloudStore: cloudStore},
		mtFlushChan: make(chan struct{}, conf.MemtableFlushQueueMaxSize),
	}
}

func (s *Shakti) Start() error {
	go s.mtFlushRunLoop()
	return nil
}

func (s *Shakti) Stop() error {
	close(s.mtFlushChan)
	return nil
}

func (s *Shakti) Write(batch *Batch) error {
	for {
		mt, ok, err := s.doWrite(batch)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}

		// No more space left in memtable - swap it out and replace it with a new one and flush it async
		s.mtLock.Lock()
		if mt == s.memtable {

			// It hasn't already been swapped so swap it
			s.memtable = NewMemtable(1024 * 1024)

			s.mtFlushQueueLock.Lock()
			s.mtFlushQueue = append(s.mtFlushQueue, mt)
			s.mtFlushQueueLock.Unlock()
			s.mtFlushChan <- struct{}{}

			// TODO we need to add the new memtable as the zeroth element in any existing merging iterators!!
		}
		s.mtLock.Unlock()
	}
}

func (s *Shakti) ReceiveReplicatedBatch(batch *Batch) error {
	return nil
}

func (s *Shakti) doWrite(batch *Batch) (*Memtable, bool, error) {
	s.mtLock.RLock()
	defer s.mtLock.RUnlock()
	mt := s.memtable
	ok, err := mt.Write(batch)
	return mt, ok, err
}

func (s *Shakti) getMemtable() *Memtable {
	s.mtLock.RLock()
	defer s.mtLock.RUnlock()
	return s.memtable
}

func (s *Shakti) NewIterator(keyStart []byte, keyEnd []byte) (Iterator, error) {

	ids, err := s.registry.GetTableIDsForRange(keyStart, keyEnd, 10000) // TODO don't hardcode
	if err != nil {
		return nil, err
	}

	// TODO we should prevent very slow or stalled iterators from holding memtables or sstables in memory too long
	// we should detect if they are very slow, and close them if they are
	s.mtLock.RLock()
	iters := make([]Iterator, len(ids) + 1 + len(s.mtFlushQueue))
	pos := 0
	iters[pos] = s.memtable.NewIterator(keyStart, keyEnd)
	pos++
	s.mtFlushQueueLock.Lock()
	for i, mt := range s.mtFlushQueue {
		iters[i + pos] = mt.NewIterator(keyStart, keyEnd)
		pos++
	}
	s.mtFlushQueueLock.Unlock()
	s.mtLock.RUnlock()

	for i, nonOverLapIDs := range ids {
		if len(nonOverLapIDs) == 1 {
			lazy, err := NewLazySSTableIterator(nonOverLapIDs[0], s.TableCache, keyStart, keyEnd)
			if err != nil {
				return nil, err
			}
			iters[i + pos] = lazy
		} else {
			// TODO - instead of getting all table ids and constructing a chain iterator with potentially millions of
			// LazySSTableIterators (e.g. in the case the range is large and there is a huge amount of data in storage)
			// We should get at most X table Ids per level, and the chain iterator knows how to extend itself by asking
			// for more ids using GetTableIDsForRange
			chainIters := make([]Iterator, len(nonOverLapIDs))
			for i, nonOverlapID := range nonOverLapIDs {
				lazy, err := NewLazySSTableIterator(nonOverlapID, s.TableCache, keyStart, keyEnd)
				if err != nil {
					return nil, err
				}
				chainIters[i] = lazy
			}
			iters[i + pos] = &ChainingIterator{iters: chainIters}
		}
		pos++
	}

	return &MergingIterator{iters: iters}, nil
}

func (s *Shakti) mtFlushRunLoop() {
	for range s.mtFlushChan {
		s.mtFlushQueueLock.Lock()
		mt := s.mtFlushQueue[0]
		s.mtFlushQueueLock.Unlock()
		if err := s.flushMemtable(mt); err != nil {
			log.Errorf("failed to flush memtable %+v", err)
		}
		// We don't remove from the flush queue until the sstable has been added to the table cache otherwise iterators
		// created during this time could miss the data
		// It's possible with this method that the memtable and the new sstable with the same data could be added to the
		// same iterator but this doesn't matter as the merging iterator will just take the newest version of the key
		// and ignore older ones (the sstable)
		s.mtFlushQueueLock.Lock()
		s.mtFlushQueue = s.mtFlushQueue[1:]
		s.mtFlushQueueLock.Unlock()
	}
}

func (s *Shakti) flushMemtable(mt *Memtable) error {

	iter := mt.NewIterator(nil, nil)
	ssTable, smallestKey, largestKey, err := BuildSSTable(mt.CommonPrefix(), iter)
	if err != nil {
		return err
	}
	id, err := uuid.New().MarshalBinary()
	if err != nil {
		return err
	}
	if err := s.TableCache.AddSSTable(id, ssTable); err != nil {
		return err
	}
	tableBytes := ssTable.Serialize(nil)
	if err := s.cloudStore.Add(id, tableBytes); err != nil {
		return err
	}
	if err := s.registry.ApplyChanges(RegistrationBatch{
		Registrations:   []RegistrationEntry{{
			Level:    0,
			TableID:  id,
			KeyStart: smallestKey,
			KeyEnd:   largestKey,
		}},
		Deregistrations: nil,
	}); err != nil {
		return err
	}
	// TODO once it's been stored and registered we can unpin it from the table cache so it's able to be evicted
	return nil
}