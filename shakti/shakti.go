package shakti

import (
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/shakti/cloudstore"
	common2 "github.com/squareup/pranadb/shakti/cmn"
	iters2 "github.com/squareup/pranadb/shakti/iteration"
	"github.com/squareup/pranadb/shakti/mem"
	"github.com/squareup/pranadb/shakti/nexus"
	"github.com/squareup/pranadb/shakti/sst"
	"sync"
	"sync/atomic"
)

type Shakti struct {
	conf         common2.Conf
	memtable     *mem.Memtable
	cloudStore   cloudstore.Store
	registry     nexus.Controller
	TableCache   *sst.Cache
	mtLock       sync.RWMutex
	mtFlushChan  chan struct{}
	mtFlushQueue []mtFlushEntry
	// We use a separate lock to protect the flush queue as we don't want removing first element from queue to block
	// writes to the memtable
	mtFlushQueueLock common.SpinLock
}

func NewShakti(store cloudstore.Store, registry nexus.Controller, conf common2.Conf) *Shakti {
	memtable := mem.NewMemtable(conf.MemtableMaxSizeBytes)
	return &Shakti{
		conf:        conf,
		memtable:    memtable,
		cloudStore:  store,
		registry:    registry,
		TableCache:  sst.NewTableCache(store),
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

func (s *Shakti) Write(batch *mem.Batch) error {
	for {
		memtable, ok, err := s.doWrite(batch)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}

		// No more space left in memtable - swap writeIter out and replace writeIter with a new one and flush writeIter async
		s.mtLock.Lock()
		if memtable == s.memtable {

			// It hasn't already been swapped so swap writeIter
			s.memtable = mem.NewMemtable(s.conf.MemtableMaxSizeBytes)

			s.mtFlushQueueLock.Lock()
			s.mtFlushQueue = append(s.mtFlushQueue, mtFlushEntry{
				memtable: memtable,
			})
			s.mtFlushQueueLock.Unlock()
			s.mtFlushChan <- struct{}{}

			// TODO we need to add the new memtable as the zeroth element in any existing merging iterators!!
		}
		s.mtLock.Unlock()
	}
}

func (s *Shakti) ReceiveReplicatedBatch(batch *mem.Batch) error {
	return nil
}

func (s *Shakti) doWrite(batch *mem.Batch) (*mem.Memtable, bool, error) {
	s.mtLock.RLock()
	defer s.mtLock.RUnlock()
	mt := s.memtable
	ok, err := mt.Write(batch)
	return mt, ok, err
}

func (s *Shakti) getMemtable() *mem.Memtable {
	s.mtLock.RLock()
	defer s.mtLock.RUnlock()
	return s.memtable
}

func (s *Shakti) NewIterator(keyStart []byte, keyEnd []byte) (iters2.Iterator, error) {

	ids, err := s.registry.GetTableIDsForRange(keyStart, keyEnd, 10000) // TODO don't hardcode
	if err != nil {
		return nil, err
	}

	// TODO we should prevent very slow or stalled iterators from holding memtables or sstables in memory too long
	// we should detect if they are very slow, and close them if they are
	s.mtLock.RLock()
	// We creating a merging iterator which merges from a set of potentially overlapping Memtables/SSTables in order
	// from newest to oldest
	iters := make([]iters2.Iterator, len(ids)+1+len(s.mtFlushQueue))
	pos := 0
	// First we add the current memtable
	iters[pos] = s.memtable.NewIterator(keyStart, keyEnd)
	pos++
	s.mtFlushQueueLock.Lock()
	// Then we add each memtable in the flush queue
	for i := len(s.mtFlushQueue) - 1; i >= 0; i-- {
		fe := s.mtFlushQueue[i]
		iters[pos] = fe.memtable.NewIterator(keyStart, keyEnd)
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
			lazy, err := sst.NewLazySSTableIterator(nonOverLapIDs[0], s.TableCache, keyStart, keyEnd)
			if err != nil {
				return nil, err
			}
			iters[i+pos] = lazy
		} else {
			// TODO - instead of getting all table ids and constructing a chain iterator with potentially millions of
			// LazySSTableIterators (e.g. in the case the range is large and there is a huge amount of data in storage)
			// We should get at most X table Ids per level, and the chain iterator knows how to extend itself by asking
			// for more ids using GetTableIDsForRange
			chainIters := make([]iters2.Iterator, len(nonOverLapIDs))
			for i, nonOverlapID := range nonOverLapIDs {
				lazy, err := sst.NewLazySSTableIterator(nonOverlapID, s.TableCache, keyStart, keyEnd)
				if err != nil {
					return nil, err
				}
				chainIters[i] = lazy
			}
			iters[i+pos] = iters2.NewChainingIterator(iters)
		}
		pos++
	}

	return iters2.NewMergingIterator(iters, false)
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

type ssTableInfo struct {
	ssTableID   sst.SSTableID
	largestKey  []byte
	smallestKey []byte
}

type mtFlushEntry struct {
	memtable  *mem.Memtable
	ssTabInfo atomic.Value
}

// Called after the ssTable for the memtable has been stored to cloud storage
func (fe *mtFlushEntry) setSSTableInfo(ssTableInfo *ssTableInfo) {
	fe.ssTabInfo.Store(ssTableInfo)
}

func (fe *mtFlushEntry) getSSTableInfo() *ssTableInfo {
	s := fe.ssTabInfo.Load()
	if s == nil {
		return nil
	}
	return s.(*ssTableInfo)
}

func (s *Shakti) mtFlushRunLoop() {
	var bufEstimates bufSizeEstimates
	pos := 0
	for range s.mtFlushChan {
		s.mtFlushQueueLock.Lock()
		var i int
		// We keep memtables in the flush queue until they are actually flushed and this happens asynchronously
		// Here we remove any contiguous range of flushed memtables and remove the flushed prefix of the flush queue
		// We make sure we register sstables in the same order they were added to the flush queue
		for i = 0; i < pos; i++ {
			tabInfo := s.mtFlushQueue[i].getSSTableInfo()
			if tabInfo == nil {
				// Not stored in cloud storage yet
				break
			} else if err := s.registry.ApplyChanges(nexus.RegistrationBatch{
				Registrations: []nexus.RegistrationEntry{{
					Level:    0,
					TableID:  tabInfo.ssTableID,
					KeyStart: tabInfo.smallestKey,
					KeyEnd:   tabInfo.largestKey,
				}},
				Deregistrations: nil,
			}); err != nil {
				log.Errorf("failed to register sstable %+v", err)
				return
			}
		}
		s.mtFlushQueue = s.mtFlushQueue[i:]
		pos -= i
		// Take next one to flush
		flushEntry := s.mtFlushQueue[pos]
		s.mtFlushQueueLock.Unlock()
		pos++
		buffSizeEstimate := bufEstimates.getMtBuffSizeEstimate()
		entriesEstimate := bufEstimates.getMtEntriesEstimate()
		// We flush in parallel as cloud storage can have a high latency
		go func() {
			buffSize, entries, err := s.flushMemtable(flushEntry, buffSizeEstimate, entriesEstimate)
			if err != nil {
				log.Errorf("failed to flush memtable %+v", err)
				return
			}
			bufEstimates.updateSizeEstimates(buffSize, entries)
		}()
	}
}

// Flush the memtable to a sstable, and push writeIter to cloud storage, this method does not register the sstable with
// the registry. Registration must be done in the same order in which memtables were created. Flushing can occur
// in parallel for multiple memtables.
func (s *Shakti) flushMemtable(flushEntry mtFlushEntry, buffSizeEstimate int, entriesEstimate int) (int, int, error) {
	mt := flushEntry.memtable
	iter := mt.NewIterator(nil, nil)
	ssTable, smallestKey, largestKey, err := sst.BuildSSTable(s.conf.TableFormat, buffSizeEstimate, entriesEstimate,
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
	flushEntry.setSSTableInfo(&ssTableInfo{
		ssTableID:   id,
		largestKey:  largestKey,
		smallestKey: smallestKey,
	})
	return len(tableBytes), ssTable.NumEntries(), nil
}
