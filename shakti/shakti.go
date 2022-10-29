package shakti

import (
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"sync"
	"sync/atomic"
	"time"
)

type Shakti struct {
	conf         Conf
	memtable     *Memtable
	cloudStore   CloudStore
	registry     Registry
	TableCache   *TableCache
	mtLock       sync.RWMutex
	mtFlushChan  chan struct{}
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
	RegistryFormat RegistryFormat
	WriteFormat               Format
	MemtableMaxSizeBytes      int
	MemtableFlushQueueMaxSize int
	SegmentDeleteDelay        time.Duration
	MasterRegistryRecordID    string
	L0FilesCompactionTrigger  int // There is one L0 per processor. This is the value per processor
	L1FilesCompactionTrigger  int // There is a single L1. This is a global value
	LevelFilesMultiplier      int // After L1, subsequence levels compaction trigger multiplies by this for each level
	CreateRegistry            bool
	MaxRegistrySegmentTableEntries    int
}

func NewShakti(cloudStore CloudStore, registry Registry, conf Conf) *Shakti {
	memtable := NewMemtable(conf.MemtableMaxSizeBytes)
	return &Shakti{
		conf:        conf,
		memtable:    memtable,
		cloudStore:  cloudStore,
		registry:    registry,
		TableCache:  &TableCache{cloudStore: cloudStore},
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

		// No more space left in memtable - swap writeIter out and replace writeIter with a new one and flush writeIter async
		s.mtLock.Lock()
		if mt == s.memtable {

			// It hasn't already been swapped so swap writeIter
			s.memtable = NewMemtable(s.conf.MemtableMaxSizeBytes)

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
	// We creating a merging iterator which merges from a set of potentially overlapping Memtables/SSTables in order
	// from newest to oldest
	iters := make([]Iterator, len(ids)+1+len(s.mtFlushQueue))
	pos := 0
	// First we add the current memtable
	iters[pos] = s.memtable.NewIterator(keyStart, keyEnd)
	pos++
	s.mtFlushQueueLock.Lock()
	// Then we add each memtable in the flush queue
	for i := len(s.mtFlushQueue) - 1; i >= 0; i-- {
		mt := s.mtFlushQueue[i]
		iters[pos] = mt.NewIterator(keyStart, keyEnd)
		pos++
	}
	s.mtFlushQueueLock.Unlock()
	s.mtLock.RUnlock()

	// Then we add each flushed SSTable with overlapping keys from the registry. It's possible we might have the included
	// the same keys twice in a memtable from the flush queue which has been already flushed and one from the registry
	// This is ok as he later one (the sstable) will just be ignored in the iterator. However TODO we could detect
	// this and not add writeIter if this is the case
	for i, nonOverLapIDs := range ids {
		if len(nonOverLapIDs) == 1 {
			lazy, err := NewLazySSTableIterator(nonOverLapIDs[0], s.TableCache, keyStart, keyEnd)
			if err != nil {
				return nil, err
			}
			iters[i+pos] = lazy
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
			iters[i+pos] = &ChainingIterator{iters: chainIters}
		}
		pos++
	}

	return &MergingIterator{iters: iters}, nil
}

type bufSizeEstimates struct {
	mtBuffSizeEstimate uint32
	mtEntriesEstimate  uint32
}

// update estimates of buffer size and number of entries - having a good estimate improves performance as writeIter reduces or
// eliminates slice copying when original capacity is exceeded. We basically take the largest we've seen and add 5%
func (b *bufSizeEstimates) updateSizeEstimates(buffSize int, entries int) {
	// Note: It doesn't matter too much if we have a race here as writeIter's just an estimate so no need to lock we can just use an
	// atomic
	if buffSize > b.getMtBuffSizeEstimate() {
		atomic.StoreUint32(&b.mtBuffSizeEstimate, uint32(float64(buffSize)*1.05))
	}
	if entries > b.getMtEntriesEstimate() {
		atomic.StoreUint32(&b.mtEntriesEstimate, uint32(float64(entries)*1.05))
	}
}

func (b *bufSizeEstimates) getMtBuffSizeEstimate() int {
	return int(atomic.LoadUint32(&b.mtBuffSizeEstimate))
}

func (b *bufSizeEstimates) getMtEntriesEstimate() int {
	return int(atomic.LoadUint32(&b.mtEntriesEstimate))
}

func (s *Shakti) mtFlushRunLoop() {
	var bufSizeEstimates bufSizeEstimates
	pos := 0
	for range s.mtFlushChan {
		s.mtFlushQueueLock.Lock()
		var i int
		// We keep memtables in the flush queue until they are actually flushed and this happens asynchronously
		// Here we remove any contiguous range of flushed memtables and remove the flushed prefix of the flush queue
		// We make sure we register sstables in the same order they were added to the flush queue
		for i = 0; i < pos; i++ {
			ssTableInfo := s.mtFlushQueue[i].getSSTableInfo()
			if ssTableInfo == nil {
				// Not stored in cloud storage yet
				break
			} else {
				if err := s.registry.ApplyChanges(RegistrationBatch{
					Registrations: []RegistrationEntry{{
						Level:    0,
						TableID:  ssTableInfo.ssTableID,
						KeyStart: ssTableInfo.smallestKey,
						KeyEnd:   ssTableInfo.largestKey,
					}},
					Deregistrations: nil,
				}); err != nil {
					log.Errorf("failed to register sstable %+v", err)
					return
				}
			}
		}
		s.mtFlushQueue = s.mtFlushQueue[i:]
		pos -= i
		// Take next one to flush
		mt := s.mtFlushQueue[pos]
		s.mtFlushQueueLock.Unlock()
		pos++
		buffSizeEstimate := bufSizeEstimates.getMtBuffSizeEstimate()
		entriesEstimate := bufSizeEstimates.getMtEntriesEstimate()
		// We flush in parallel as cloud storage can have a high latency
		go func() {
			buffSize, entries, err := s.flushMemtable(mt, buffSizeEstimate, entriesEstimate)
			if err != nil {
				log.Errorf("failed to flush memtable %+v", err)
				return
			}
			bufSizeEstimates.updateSizeEstimates(buffSize, entries)
		}()
	}
}

// Flush the memtable to a sstable, and push writeIter to cloud storage, this method does not register the sstable with
// the registry. Registration must be done in the same order in which memtables were created. Flushing can occur
// in parallel for multiple memtables.
func (s *Shakti) flushMemtable(mt *Memtable, buffSizeEstimate int, entriesEstimate int) (int, int, error) {
	iter := mt.NewIterator(nil, nil)
	ssTable, smallestKey, largestKey, err := BuildSSTable(s.conf.WriteFormat, buffSizeEstimate, entriesEstimate,
		mt.CommonPrefix(), iter)
	if err != nil {
		return 0, 0, err
	}
	id, err := uuid.New().MarshalBinary()
	if err != nil {
		return 0, 0, err
	}
	if err := s.TableCache.AddSSTable(id, ssTable); err != nil {
		return 0, 0, err
	}
	tableBytes := ssTable.Serialize()
	if err := s.cloudStore.Add(id, tableBytes); err != nil {
		return 0, 0, err
	}
	mt.setSSTableInfo(&SSTableInfo{
		ssTableID:   id,
		largestKey:  largestKey,
		smallestKey: smallestKey,
	})
	return len(tableBytes), ssTable.numEntries, nil
}
