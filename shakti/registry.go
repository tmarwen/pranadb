package shakti

import (
	"bytes"
	"github.com/squareup/pranadb/common"
	"sync"
)

type Registry interface {
	GetTableIDsForRange(keyStart []byte, keyEnd []byte, maxTablesPerLevel int) (OverlappingTableIDs, error)
	ApplyChanges(registrationBatch RegistrationBatch) error
	Start() error
	Stop() error
}

type NonoverlappingTableIDs []SSTableID
type OverlappingTableIDs []NonoverlappingTableIDs

type registry struct {
	lock sync.RWMutex
	cloudStore CloudStore
	masterRecordID []byte
	segmentCache sync.Map // TODO make this a LRU cache
	masterRecord *masterRecord
}

func NewRegistry(cloudStore CloudStore, masterRecordID []byte) Registry {
	return &registry{
		masterRecordID: masterRecordID,
		cloudStore: cloudStore,
	}
}

func (r *registry) Start() error {
	bytes, err := r.cloudStore.Get(r.masterRecordID)
	if err != nil {
		return err
	}
	mr := &masterRecord{}
	if err := mr.deserialize(bytes); err != nil {
		return err
	}
	r.masterRecord = mr
	return nil
}

func (r *registry) Stop() error {
	return nil
}

func (r *registry) getSegment(segmentID []byte) (*segment, error) {
	skey := common.ByteSliceToStringZeroCopy(segmentID)
	seg, ok := r.segmentCache.Load(skey)
	if ok {
		return seg.(*segment), nil
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	seg, ok = r.segmentCache.Load(skey)
	if ok {
		return seg.(*segment), nil
	}
	buff, err := r.cloudStore.Get(segmentID)
	if err != nil {
		return nil, err
	}
	segment := &segment{}
	if err := segment.deserialize(buff); err != nil {
		return nil, err
	}
	r.segmentCache.Store(skey, segment)
	return segment, nil
}

func (r *registry) GetTableIDsForRange(keyStart []byte, keyEnd []byte, maxTablesPerLevel int) (OverlappingTableIDs, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	// Dumb slow implementation
	var overlapping OverlappingTableIDs
	for i := 0; i < len(r.masterRecord.levelSegmentIds); i++ {

		segmentIds := r.masterRecord.levelSegmentIds[i]
		var tableIDs []SSTableID
		for _, segmentID := range segmentIds {
			if hasOverlap(keyStart, keyEnd, segmentID.rangeStart, segmentID.rangeEnd) {
				segment, err := r.getSegment(segmentID.segmentID)
				if err != nil {
					return nil, err
				}
				for _, tableEntry := range segment.tableEntries {
					if hasOverlap(keyStart, keyEnd, tableEntry.rangeStart, tableEntry.rangeEnd) {
						tableIDs = append(tableIDs, tableEntry.ssTableID)
					}
				}
			}
		}

		if i == 0 {
			// Level 0 is overlapping
			for _, tableID := range tableIDs {
				overlapping = append(overlapping, []SSTableID{tableID})
			}
		} else if tableIDs != nil {
			// Other levels are non overlapping
			overlapping = append(overlapping, tableIDs)
		}

	}
	return overlapping, nil
}

func (r *registry) ApplyChanges(registrationBatch RegistrationBatch) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	/*
	1. replicate batch
	2. apply changes - keep track of any segments added deleted since last flush
	3. every so often (e.g. every 10 seconds), push new segments, then push new master
	4. add segments to delete to to_delete queue and delete them after a delay
	 */

	return nil
}

type RegistrationEntry struct {
	Level int
	TableID SSTableID
	KeyStart, KeyEnd []byte
}

type DeregistrationEntry struct {
	Level int
	TableID SSTableID
}

type RegistrationBatch struct {
	Registrations []RegistrationEntry
	Deregistrations []DeregistrationEntry
}

type registryState struct {
	segmentCache sync.Map
	masterRecord *masterRecord
}

type segmentID []byte

type segment struct {
	rangeStart []byte
	rangeEnd []byte
	tableEntries []*tableEntry
}

func (s *segment) deserialize(buff []byte) error {
	// TODO
	return nil
}

type segmentEntry struct {
	segmentID segmentID
	rangeStart []byte
	rangeEnd []byte
	// TODO maybe have bloom filter here too??
}

type tableEntry struct {
	ssTableID SSTableID
	rangeStart []byte
	rangeEnd []byte
	bloomFilter []byte
}

type masterRecord struct {
	levelSegmentIds map[int][]*segmentEntry
}

func (mr *masterRecord) deserialize(buff []byte) error {
	// TODO
	return nil
}

func (mr *masterRecord) getSegmentIds(level int, rangeStart []byte, rangeEnd []byte) [][]byte {
	return nil
}

func hasOverlap(keyStart []byte, keyEnd []byte, blockKeyStart []byte, blockKeyEnd []byte) bool {
	// Note! keyStart is inclusive, keyEnd is exclusive
	// registry keyStart and keyEnd are inclusive!
	dontOverlap := bytes.Compare(keyStart, blockKeyEnd) > 0 || bytes.Compare(keyEnd, blockKeyStart) <= 0
	return !dontOverlap
}


