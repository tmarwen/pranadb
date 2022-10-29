package shakti

import (
	"bytes"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"sync"
	"time"
)

type Registry interface {
	GetTableIDsForRange(keyStart []byte, keyEnd []byte, maxTablesPerLevel int) (OverlappingTableIDs, error)
	ApplyChanges(registrationBatch RegistrationBatch) error
	Start() error
	Stop() error
}

type NonoverlappingTableIDs []SSTableID
type OverlappingTableIDs []NonoverlappingTableIDs

type RegistryFormat byte

type registry struct {
	lock                 sync.RWMutex
	format RegistryFormat
	cloudStore           CloudStore
	conf                 Conf
	segmentCache         sync.Map // TODO make this a LRU cache
	masterRecord         *masterRecord
	segmentsToAdd        map[string]*segment
	segmentsToDelete     []segmentDeleteEntry
	segmentsToDeleteLock sync.Mutex
	masterRecordBufferSizeEstimate int
	segmentBufferSizeEstimate int
}

func NewRegistry(cloudStore CloudStore, conf Conf) Registry {
	return &registry{
		format: conf.RegistryFormat,
		cloudStore: cloudStore,
		conf:       conf,
	}
}

func (r *registry) Start() error {
	buff, err := r.cloudStore.Get([]byte(r.conf.MasterRegistryRecordID))
	if err != nil {
		return err
	}
	if buff != nil {
		if r.conf.CreateRegistry {
			return errors.NewPranaErrorf(errors.InvalidConfiguration,
				"CreateRegistry specified but registry with id %s already exists", r.conf.MasterRegistryRecordID)
		}
		mr := &masterRecord{}
		mr.deserialize(buff, 0)
		r.masterRecord = mr
	} else if r.conf.CreateRegistry {
		r.masterRecord = &masterRecord{levelSegmentEntries: map[int][]*segmentEntry{}}
		buff := r.masterRecord.serialize(nil)
		if err := r.cloudStore.Add([]byte(r.conf.MasterRegistryRecordID), buff); err != nil {
			return err
		}
		log.Infof("created prana registry with id %s", r.conf.MasterRegistryRecordID)
	}
	return nil
}

func (r *registry) updateMasterRecordBufferSizeEstimate(buffSize int) {
	if buffSize > r.masterRecordBufferSizeEstimate {
		r.masterRecordBufferSizeEstimate = int(float64(buffSize) * 1.05)
	}
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
	if buff == nil {
		return nil, errors.Errorf("cannot find segment with id %d", segmentID)
	}
	segment := &segment{}
	segment.deserialize(buff)
	r.segmentCache.Store(skey, segment)
	return segment, nil
}

func (r *registry) findTableIDsWithOverlapInSegment(seg *segment, keyStart []byte, keyEnd []byte) []SSTableID {
	var tableIDs []SSTableID
	for _, tabEntry := range seg.tableEntries {
		if hasOverlap(keyStart, keyEnd, tabEntry.rangeStart, tabEntry.rangeEnd) {
			tableIDs = append(tableIDs, tabEntry.ssTableID)
		}
	}
	return tableIDs
}

func (r *registry) GetTableIDsForRange(keyStart []byte, keyEnd []byte, maxTablesPerLevel int) (OverlappingTableIDs, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	var overlapping OverlappingTableIDs
	for i := 0; i < len(r.masterRecord.levelSegmentEntries); i++ {
		segmentEntries := r.masterRecord.levelSegmentEntries[i]
		var tableIDs []SSTableID
		for _, segEntry := range segmentEntries {
			if hasOverlap(keyStart, keyEnd, segEntry.rangeStart, segEntry.rangeEnd) {
				seg, err := r.getSegment(segEntry.segmentID)
				if err != nil {
					return nil, err
				}
				// TODO For L0 where there is overlap and a single segment, we could use an interval tree
				// TODO for L > 0 where there is no overlap we can use binary search to locate the segments
				// and also to locate the table ids in the segment
				tableIDs = r.findTableIDsWithOverlapInSegment(seg, keyStart, keyEnd)
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

	if err := r.replicateBatch(registrationBatch); err != nil {
		return err
	}

	// We must process the deregistrations first or we can temporarily have overlapping keys if we did the adds first
	if err := r.applyDeregistrations(registrationBatch.Deregistrations); err != nil {
		return err
	}

	if err := r.applyRegistrations(registrationBatch.Registrations); err != nil {
		return err
	}

	return r.maybeTriggerCompaction()
}

func (r *registry) applyDeregistrations(registrations []DeregistrationEntry) error {
	// TODO
	return nil
}

func (r *registry) applyRegistrations(registrations []RegistrationEntry) error {
	for _, registration := range registrations {
		// The new table entry that we're going to add
		tabEntry := &tableEntry{
			ssTableID:   registration.TableID,
			rangeStart:  registration.KeyStart,
			rangeEnd:    registration.KeyEnd,
		}
		segmentEntries, ok := r.masterRecord.levelSegmentEntries[registration.Level]
		if registration.Level == 0 {
			// We have overlapping keys in L0 so we just append to the last segment
			var seg *segment
			var segRangeStart, segRangeEnd []byte
			if ok {
				// Segment already exists
				// Level 0 only ever has one segment
				l0SegmentEntry := segmentEntries[0]
				var err error
				seg, err = r.getSegment(l0SegmentEntry.segmentID)
				if err != nil {
					return err
				}
				seg.tableEntries = append(seg.tableEntries, tabEntry)
				segRangeStart = l0SegmentEntry.rangeStart
				segRangeEnd = l0SegmentEntry.rangeEnd
				// Update the ranges
				if bytes.Compare(registration.KeyStart, segRangeStart) < 0 {
					segRangeStart = registration.KeyStart
				}
				if bytes.Compare(registration.KeyEnd, segRangeEnd) > 0 {
					segRangeEnd = registration.KeyEnd
				}
			} else {
				// Create a new segment
				seg = &segment{
					tableEntries: []*tableEntry{tabEntry},
				}
				segRangeStart = registration.KeyStart
				segRangeEnd = registration.KeyEnd
			}

			id, err := r.storeNewSegment(seg)
			if err != nil {
				return err
			}

			// Update the master record
			r.masterRecord.levelSegmentEntries[0] = []*segmentEntry{{
				segmentID:  id,
				rangeStart: segRangeStart,
				rangeEnd:   segRangeEnd,
			}}
		} else {
			// L > 0
			// Segments in these levels are non overlapping

			// Find which segment the new registration belongs in
			// TODO binary search
			found := -1
			for i := 0; i < len(segmentEntries); i++ {
				// If the new table key start is after the key end of the previous segment (or there is no previous segment)
				// and the new table key end is before the key start of the next segment (or there is no next segment)
				// then we add the table entry to the current segment
				if (i == 0 || bytes.Compare(registration.KeyStart, segmentEntries[i - 1].rangeEnd) > 0) &&
					(i != len(segmentEntries) - 1 || bytes.Compare(registration.KeyEnd, segmentEntries[i + 1].rangeStart) < 0) {
					found = i
					break
				}
			}
			if len(segmentEntries) > 0 && found == -1 {
				panic("cannot find segment for new table entry")
			}
			if found != -1 {
				// We know the segment in which the new table entry will live, so we need to load it,
				// find the place to insert the new entry (all entries are in keyStart order with no overlap)
				// insert it, then maybe split the segment into two if it is too big, store the new segment(s)
				// and update the segment entries for the level
				segEntry := segmentEntries[found]
				seg, err := r.getSegment(segEntry.segmentID)
				if err != nil {
					return err
				}
				// TODO some kind of binary search??
				// Find the insert point
				insertPoint := -1
				for i, te := range seg.tableEntries {
					if bytes.Compare(registration.KeyEnd, te.rangeStart) < 0 {
						insertPoint = i
						break
					}
				}
				// Create the new table entries for the segment
				var newTableEntries []*tableEntry
				if insertPoint >= 0 {
					left := seg.tableEntries[:insertPoint]
					right := seg.tableEntries[insertPoint:]
					newTableEntries = append(newTableEntries, left...)
					newTableEntries = append(newTableEntries, tabEntry)
					newTableEntries = append(newTableEntries, right...)
				} else if insertPoint == -1 {
					newTableEntries = append(newTableEntries, seg.tableEntries...)
					newTableEntries = append(newTableEntries, tabEntry)
				}

				// Create the new segment(s)
				var newSegs []segment
				if len(newTableEntries) > r.conf.MaxRegistrySegmentTableEntries {
					// Too many entries so we split the segment in two
					mid := len(newTableEntries) / 2
					te1 := newTableEntries[:mid]
					te2 := newTableEntries[mid:]
					newSegs = append(newSegs, segment{format:byte(r.format),tableEntries: te1}, segment{format:byte(r.format),tableEntries: te2})
				} else {
					newSegs = []segment{{format:byte(r.format), tableEntries: newTableEntries}}
				}

				// Store the new segment(s)
				newEntries := make([]*segmentEntry, len(newSegs))
				for i, seg := range newSegs {
					id, err := r.storeNewSegment(&seg)
					if err != nil {
						return err
					}
					newEntries[i] = &segmentEntry{
						segmentID:  id,
						rangeStart: seg.tableEntries[0].rangeStart,
						rangeEnd:   seg.tableEntries[len(seg.tableEntries) - 1].rangeEnd,
					}
				}

				// Create the new segment entries
				newSegEntries := make([]*segmentEntry, 0, len(segmentEntries) - 1 + len(newSegs))
				newSegEntries = append(newSegEntries, segmentEntries[:found]...)
				newSegEntries = append(newSegEntries, newEntries...)
				if found != len(segmentEntries) - 1 {
					newSegEntries = append(newSegEntries, segmentEntries[found+1:]...)
				}

				// Update the master record
				r.masterRecord.levelSegmentEntries[registration.Level] = newSegEntries
			} else {
				// The first segment in the level
				seg := &segment{tableEntries: []*tableEntry{tabEntry}}
				id, err := r.storeNewSegment(seg)
				if err != nil {
					return err
				}
				segEntry := &segmentEntry{
					segmentID:  id,
					rangeStart: registration.KeyStart,
					rangeEnd:   registration.KeyEnd,
				}
				r.masterRecord.levelSegmentEntries[registration.Level] = []*segmentEntry{segEntry}
			}
		}
	}
	return nil
}

func (r *registry) storeNewSegment(seg *segment) ([]byte, error) {
	id, err := uuid.New().MarshalBinary()
	if err != nil {
		return nil, err
	}
	// These will be added async
	r.segmentsToAdd[string(id)] = seg
	return id, nil
}

func (r *registry) replicateBatch(batch RegistrationBatch) error {
	// TODO
	return nil
}

type segmentDeleteEntry struct {
	deleteTime time.Time
	segmentID  segmentID
}

func (r *registry) queueSegmentToDelete(segmentID segmentID) {
	r.segmentsToDeleteLock.Lock()
	defer r.segmentsToDeleteLock.Unlock()
	deleteTime := time.Now().Add(r.conf.SegmentDeleteDelay)
	r.segmentsToDelete = append(r.segmentsToDelete, segmentDeleteEntry{
		deleteTime: deleteTime,
		segmentID:  segmentID,
	})
}

// Call this on a timer
func (r *registry) checkSegmentsToDelete() error {
	var toDeleteNow []segmentID
	r.segmentsToDeleteLock.Lock()
	now := time.Now()
	i := 0
	var entry segmentDeleteEntry
	for i, entry = range r.segmentsToDelete {
		if !entry.deleteTime.After(now) {
			toDeleteNow = append(toDeleteNow, entry.segmentID)
		} else {
			break
		}
	}
	r.segmentsToDeleteLock.Unlock()

	// Note: we delete outside the lock so we don't block changes to the registry
	// TODO delete in parallel
	for _, segmentID := range toDeleteNow {
		if err := r.cloudStore.Delete(segmentID); err != nil {
			return err
		}
	}

	r.segmentsToDeleteLock.Lock()
	r.segmentsToDelete = r.segmentsToDelete[i:]
	r.segmentsToDeleteLock.Unlock()

	return nil
}

/*
Flush the changes to cloud storage, we keep track of the segments to add and remove since the last flush
*/
func (r *registry) flushChanges() error {

	/*
	Instead of this we should add segment to Flush to a channel
	and then have a goroutine consuming from that and writing them in parallel

	We should also ensure changes to the master Record aren't available to read until they've all been replicated.

	1. Replicate the actual change batch to R nodes
	2. When received on replica store it in a slice of change batches and log them to log file
	3. When has been replicated to R nodes, then apply the change to the local state

	On recovery load the master record and apply the changes from the slice

	 */

	//// Make a copy of the master record and the segments we're going to flush
	//// We don't want to block changes to the registry while we're flushing
	//var mrCopy masterRecord
	//var segmentsToAdd map[string]segment
	//r.lock.Lock()
	//mrCopy = *r.masterRecord
	//segmentsToAdd = r.segmentsToAdd
	//r.segmentsToAdd = make(map[string]segment)
	//r.lock.Unlock()
	//
	//// First we push the new segments
	//for sSegmentID, segment := range segmentsToAdd {
	//	segmentID := common.StringToByteSliceZeroCopy(sSegmentID)
	//	bytes, err := segment.serialize()
	//	if err != nil {
	//		return err
	//	}
	//	// TODO add them in parallel
	//	if err := r.cloudStore.Add(segmentID, bytes); err != nil {
	//		return err
	//	}
	//}
	//// Once they've all been added we can flush the master record
	//bytes, err := mrCopy.serialize()
	//if err != nil {
	//	return err
	//}
	//return r.cloudStore.Add(r.conf.MasterRegistryRecordID, bytes)
	return nil
}

func (r *registry) maybeTriggerCompaction() error {
	/*
		Check if too many files in levels, and if so schedule a compaction
		Keep track of compactions already scheduled but not run yet so we don't schedule them more than once

		How do we choose which tables to compact for L > 0?

		In this case tables in the level have no overlapping keys, and are laid out in segments in key order
		We choose a table to compact by selecting the table with the next highest key range to the last one compacted in that
		level, if there is no such table we wrap around to the lowest key ranged table in the level

		Once we have chosen a table we find the set of tables with any key overlap in the next level, these are then sent to the compactor
		which outputs one or more sstables, these are then stored in cloud storage and then applied to registry in a change batch
		which also includes deletion of the old sstable from the previous level and old tables from the higher level.
	*/
	return nil
}

type RegistrationEntry struct {
	Level            int
	TableID          SSTableID
	KeyStart, KeyEnd []byte
}

type DeregistrationEntry struct {
	Level   int
	TableID SSTableID
}

type RegistrationBatch struct {
	Registrations   []RegistrationEntry
	Deregistrations []DeregistrationEntry
}

type segmentID []byte

type segment struct {
	format byte // Placeholder to allow us to change format later on
	tableEntries []*tableEntry
}

func (s *segment) serialize(buff []byte) []byte {
	buff = append(buff, s.format)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(s.tableEntries)))
	for _, te := range s.tableEntries {
		buff = te.serialize(buff)
	}
	return buff
}

func (s *segment) deserialize(buff []byte) {
	s.format = buff[0]
	offset := 1
	var l uint32
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	s.tableEntries = make([]*tableEntry, int(l))
	for _, te := range s.tableEntries {
		offset = te.deserialize(buff, offset)
	}
}

type tableEntry struct {
	ssTableID   SSTableID
	rangeStart  []byte
	rangeEnd    []byte
}

func (te *tableEntry) serialize(buff []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(te.ssTableID)))
	buff = append(buff, te.ssTableID...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(te.rangeStart)))
	buff = append(buff, te.rangeStart...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(te.rangeEnd)))
	buff = append(buff, te.rangeEnd...)
	return buff
}

func (te *tableEntry) deserialize(buff []byte, offset int) int {
	var l uint32
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	te.ssTableID = buff[offset: offset + int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	te.rangeStart = buff[offset: offset + int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	te.rangeEnd = buff[offset: offset + int(l)]
	offset += int(l)
	return offset
}

type segmentEntry struct {
	format byte // Placeholder to allow us to change format later on, e.g. add bloom filter in the future
	segmentID  segmentID
	rangeStart []byte
	rangeEnd   []byte
}

func (se *segmentEntry) serialize(buff []byte) []byte {
	buff = append(buff, se.format)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(se.segmentID)))
	buff = append(buff, se.segmentID...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(se.rangeStart)))
	buff = append(buff, se.rangeStart...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(se.rangeEnd)))
	buff = append(buff, se.rangeEnd...)
	return buff
}

func (se *segmentEntry) deserialize(buff []byte, offset int) int {
	se.format = buff[offset]
	offset++
	var l uint32
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	se.segmentID = buff[offset: offset + int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	se.rangeStart = buff[offset: offset + int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	se.rangeEnd = buff[offset: offset + int(l)]
	offset += int(l)
	return offset
}

type masterRecord struct {
	format byte
	levelSegmentEntries map[int][]*segmentEntry
}

func (mr *masterRecord) serialize(buff []byte) []byte {
	buff = append(buff, mr.format)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(mr.levelSegmentEntries)))
	for level, segmentEntries := range mr.levelSegmentEntries {
		buff = common.AppendUint32ToBufferLE(buff, uint32(level))
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(segmentEntries)))
		for _, segEntry := range segmentEntries {
			buff = segEntry.serialize(buff)
		}
	}
	return buff
}

func (mr *masterRecord) deserialize(buff []byte, offset int) int {
	mr.format = buff[offset]
	offset++
	var nl uint32
	nl, offset = common.ReadUint32FromBufferLE(buff, offset)
	mr.levelSegmentEntries = make(map[int][]*segmentEntry, nl)
	for i := 0; i < int(nl); i++ {
		var level, numEntries uint32
		level, offset = common.ReadUint32FromBufferLE(buff, offset)
		numEntries, offset = common.ReadUint32FromBufferLE(buff, offset)
		segEntries := make([]*segmentEntry, numEntries)
		for j := 0; j < int(numEntries); j++ {
			segEntry := &segmentEntry{}
			offset = segEntry.deserialize(buff, offset)
			segEntries[j] = segEntry
		}
		mr.levelSegmentEntries[int(level)] = segEntries
	}
	return offset
}

func hasOverlap(keyStart []byte, keyEnd []byte, blockKeyStart []byte, blockKeyEnd []byte) bool {
	// Note! keyStart is inclusive, keyEnd is exclusive
	// registry keyStart and keyEnd are inclusive!
	dontOverlap := bytes.Compare(keyStart, blockKeyEnd) > 0 || bytes.Compare(keyEnd, blockKeyStart) <= 0
	return !dontOverlap
}
