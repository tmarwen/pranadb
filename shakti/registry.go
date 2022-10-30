package shakti

import (
	"bufio"
	"bytes"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"io"
	"os"
	"sync"
)

type Registry interface {
	GetTableIDsForRange(keyStart []byte, keyEnd []byte, maxTablesPerLevel int) (OverlappingTableIDs, error)
	ApplyChanges(registrationBatch RegistrationBatch) error
	SetLeader() error
	Start() error
	Stop() error
	Validate(validateTables bool) error
}

type NonoverlappingTableIDs []SSTableID
type OverlappingTableIDs []NonoverlappingTableIDs

type RegistryFormat byte

const (
	RegistryFormatV1 RegistryFormat = 1
)

type Replicator interface {
	ReplicateMessage(message []byte) error
}

type Replica interface {
	ReceiveReplicationMessage(message []byte) error
}

type registry struct {
	lock                 sync.RWMutex
	format RegistryFormat
	cloudStore           CloudStore
	conf                 Conf
	segmentCache         sync.Map // TODO make this a LRU cache
	masterRecord         *masterRecord
	segmentsToAdd        map[string]*segment
	masterRecordToFlush  *masterRecord
	segmentsToDeleteLock sync.Mutex
	masterRecordBufferSizeEstimate int
	segmentBufferSizeEstimate int
	flushLock sync.Mutex
	replicator Replicator
	logFile *os.File
	receivedFlush bool
	leader bool
}

func NewRegistry(conf Conf, cloudStore CloudStore, replicator Replicator) Registry {
	return &registry{
		format: conf.RegistryFormat,
		cloudStore: cloudStore,
		conf:       conf,
		replicator: replicator,
	}
}

func (r *registry) Start() error {
	buff, err := r.cloudStore.Get([]byte(r.conf.MasterRegistryRecordID))
	if err != nil {
		return err
	}
	if buff != nil {
		mr := &masterRecord{}
		mr.deserialize(buff, 0)
		r.masterRecord = mr
	} else {
		r.masterRecord = &masterRecord{levelSegmentEntries: map[int][]segmentEntry{}}
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

func (r *registry) updateSegmentBufferSizeEstimate(buffSize int) {
	if buffSize > r.segmentBufferSizeEstimate {
		r.segmentBufferSizeEstimate = int(float64(buffSize) * 1.05)
	}
}

func (r *registry) Stop() error {
	return nil
}

func (r *registry) SetLeader() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.leader {
		return errors.New("already leader")
	}
	r.leader = true
	return nil
}

func (r *registry) getSegment(segmentID []byte) (*segment, error) {
	skey := common.ByteSliceToStringZeroCopy(segmentID)
	seg, ok := r.segmentCache.Load(skey)
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

// TODO implement maxTablesPerLevel!!
func (r *registry) GetTableIDsForRange(keyStart []byte, keyEnd []byte, maxTablesPerLevel int) (OverlappingTableIDs, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if !r.leader {
		return nil, errors.New("not leader")
	}
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

	if !r.leader {
		return errors.New("not leader")
	}

	if err := r.replicateBatch(r.masterRecord.version, &registrationBatch); err != nil {
		return err
	}

	newSegments := make(map[string]*segment)

	// We must process the deregistrations before registrations or we can temporarily have overlapping keys
	if err := r.applyDeregistrations(registrationBatch.Deregistrations, newSegments); err != nil {
		return err
	}

	if err := r.applyRegistrations(registrationBatch.Registrations, newSegments); err != nil {
		return err
	}

	mrCopy := r.masterRecord.copy()
	r.masterRecord.version++

	r.flushLock.Lock()
	defer r.flushLock.Unlock()

	r.masterRecordToFlush = mrCopy
	r.segmentsToAdd = newSegments

	return nil
}

func (r *registry) applyDeregistrations(deregistrations []RegistrationEntry, newSegments map[string]*segment) error {
	for _, deregistration := range deregistrations {
		segmentEntries, ok := r.masterRecord.levelSegmentEntries[deregistration.Level]
		if !ok {
			return errors.Errorf("cannot find level %d for deregistration", deregistration.Level)
		}
		// Find which segment entry the table is in
		found := -1
		for i, entry := range segmentEntries {
			// TODO binary search
			if bytes.Compare(deregistration.KeyStart, entry.rangeStart) >= 0 && bytes.Compare(deregistration.KeyEnd, entry.rangeEnd) <= 0 {
				found = i
				break
			}
		}
		if found == -1 {
			return errors.Errorf("cannot find segment for deregistration of table %v", deregistration.TableID)
		}
		// Find the table entry in the segment entry
		segEntry := segmentEntries[found]
		// Load the segment
		seg, err := r.getSegment(segEntry.segmentID)
		if err != nil {
			return err
		}

		pos := -1
		for i, te := range seg.tableEntries {
			// TODO binary search
			if bytes.Equal(deregistration.KeyStart, te.rangeStart) {
				pos = i
				break
			}
		}
		if pos == -1 {
			return errors.Error("cannot find table entry in segment")
		}
		newTableEntries := seg.tableEntries[:pos]
		if pos != len(seg.tableEntries) - 1 {
			newTableEntries = append(newTableEntries, seg.tableEntries[pos+1:]...)
		}

		// TODO if newTableEntries is small or empty then delete segment or merge it with adjacent segment

		newSeg := &segment{
			format:       seg.format,
			tableEntries: newTableEntries,
		}
		id, err := r.storeNewSegment(newSeg, newSegments)
		if err != nil {
			return err
		}
		newStart := newTableEntries[0].rangeStart
		newEnd := newTableEntries[len(newTableEntries) - 1].rangeEnd
		newSegEntry := segmentEntry{
			format:     segEntry.format,
			segmentID:  id,
			rangeStart: newStart,
			rangeEnd:   newEnd,
		}
		segmentEntries[found] = newSegEntry
		r.masterRecord.levelSegmentEntries[deregistration.Level] = segmentEntries
	}
	return nil
}

func (r *registry) applyRegistrations(registrations []RegistrationEntry, newSegments map[string]*segment) error {
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

			id, err := r.storeNewSegment(seg, newSegments)
			if err != nil {
				return err
			}

			// Update the master record
			r.masterRecord.levelSegmentEntries[0] = []segmentEntry{{
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
				newEntries := make([]segmentEntry, len(newSegs))
				for i, seg := range newSegs {
					id, err := r.storeNewSegment(&seg, newSegments)
					if err != nil {
						return err
					}
					newEntries[i] = segmentEntry{
						segmentID:  id,
						rangeStart: seg.tableEntries[0].rangeStart,
						rangeEnd:   seg.tableEntries[len(seg.tableEntries) - 1].rangeEnd,
					}
				}

				// Create the new segment entries
				newSegEntries := make([]segmentEntry, 0, len(segmentEntries) - 1 + len(newSegs))
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
				id, err := r.storeNewSegment(seg, newSegments)
				if err != nil {
					return err
				}
				segEntry := segmentEntry{
					segmentID:  id,
					rangeStart: registration.KeyStart,
					rangeEnd:   registration.KeyEnd,
				}
				r.masterRecord.levelSegmentEntries[registration.Level] = []segmentEntry{segEntry}
			}
		}
	}
	return nil
}

func (r *registry) storeNewSegment(seg *segment, segments map[string]*segment) ([]byte, error) {
	id, err := uuid.New().MarshalBinary()
	if err != nil {
		return nil, err
	}
	// Add to the cache
	r.segmentCache.Store(common.ByteSliceToStringZeroCopy(id), seg)
	// These will be added async
	segments[string(id)] = seg
	return id, nil
}

func (r *registry) ReceiveReplicationMessage(message []byte) error {
	/*
	replication message is
	flush: 1 byte, 0 = false, true otherwise
	version 8 bytes - (uint64 LE)
	crc32 4 bytes (uint32 LE)
	batch length 4 bytes (uint32 LE)
	batch bytes
	 */

	r.lock.Lock()
	defer r.lock.Unlock()

	flush := true
	if message[0] == 0 {
		flush = false
	}
	if flush {
		// The master record corresponding to all the batches in the log has been stored permanently so we can
		// close the log file and re-open it, truncating it to the beginning.
		// TODO benchmark the close and open to see how long it takes
		if err := r.logFile.Close(); err != nil {
			return err
		}
		if err := r.openOrCreateLogFile(r.conf.LogFileName); err != nil {
			return err
		}
		r.receivedFlush = true
		return nil
	}
	if !r.receivedFlush {
		// We ignore any replicated batches until we have received the first flush
		return nil
	}
	// We append the rest of the message direct to the log, avoiding deserialization/serialization cost
	_, err := r.logFile.Write(message[1:])
	if err != nil {
		return err
	}
	return nil
}

func (r *registry) openOrCreateLogFile(filename string) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	r.logFile = f
	return nil
}

func (r *registry) replicateBatch(version uint64, batch *RegistrationBatch) error {
	buff := make([]byte, 0) // TODO better buff size estimate
	buff = append(buff, 0) // flush = false
	buff = common.AppendUint64ToBufferLE(buff, version)
	buff = common.AppendUint32ToBufferLE(buff, 0) // CRC32 TODO
	buff = batch.serialize(buff)
	return r.replicator.ReplicateMessage(buff)
}

func (r *registry) recover() (bool, error) {
	// Assumes latest master record has been loaded before this is called
	f, err := os.OpenFile(r.conf.LogFileName, os.O_RDONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	latestVersion := r.masterRecord.version
	br := bufio.NewReader(f)
	var recoveredBatches []RegistrationBatch
	for {

		buff := make([]byte, 4)
		_, err = io.ReadFull(br, buff)
		if err != nil {
			// TODO
		}
		// TODO TODO TODO
		// read version from record
		var ver uint64 = 234
		if ver != latestVersion {
			break
		}
		// Read batch and add to recoveredBatches
	}

	// TODO
	// The new leader might not have local log entries for the current version, e.g. if it joined the cluster recently
	// and hadn't received the previous flush, so we ask other members of the group to see if they have the data
	// If other nodes respond with batches at the same as latest master record version that mean that data needs to be
	// applied and a new master record flushed (note the version on the replication batch is the version of the *next* unflushed
	// master record!)

	log.Printf("%v", recoveredBatches)

	// TODO
	// Apply recovered batches and flush segments and masterrecord
	// Then leader can start
	return true, nil

}

func (r *registry) flushChanges() error {

	/*
	When we apply a batch we first replicate the batch, then we process the batch on the leader, updating the in-memory
	state. The data is then available to be read, as the batch has been replicated to multiple replicas.

	If a failure occurs the batches will be processed on the new leader before it becomes live and the registry will
	end up in the same state. It doesn't matter if UUIDs of segments are different as these are not exposed to the user
	of the registry.

	During processing of the batch, there will be a set of new segments add and a new version of the master record at
	a particular version. We will add these segments to a map of segments to push and we will take a copy of the master record
	and store that in a member variable.

	Periodically, we will take a copy of the segment to push map and the stored master record copy and reset them.
	We will then push the segments, and push the master record. Then a flush can be replicated to replicas which can clear their batch log up to
	that version of the master record.

	If too many segments to push build up, then we will lock updates and force a flush to prevent the backlog growing too fast
	*/

	r.flushLock.Lock()
	mr := r.masterRecordToFlush
	segs := r.segmentsToAdd
	r.masterRecordToFlush = nil
	r.segmentsToAdd = nil
	r.flushLock.Unlock()

	if mr != nil {
		// First we push the new segments
		// Note we don't delete any segments here, that is done async some time later - this avoids the case where
		// GetTableIDsForRange is called to create an iterator but immediately some of the tables disappear because
		// of compaction (most likely of L0 which is very frequent)
		for sSegmentID, seg := range segs {
			segID := common.StringToByteSliceZeroCopy(sSegmentID)
			buff := make([]byte, r.segmentBufferSizeEstimate)
			buff = seg.serialize(buff)
			r.updateSegmentBufferSizeEstimate(len(buff))
			// TODO add them in parallel
			if err := r.cloudStore.Add(segID, buff); err != nil {
				return err
			}
		}
		// Once they've all been added we can flush the master record
		buff := make([]byte, r.masterRecordBufferSizeEstimate)
		buff = mr.serialize(buff)
		r.updateMasterRecordBufferSizeEstimate(len(buff))
		return r.cloudStore.Add([]byte(r.conf.MasterRegistryRecordID), buff)
	}
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

func (re *RegistrationEntry) serialize(buff []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(re.Level))
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(re.TableID)))
	buff = append(buff, re.TableID...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(re.KeyStart)))
	buff = append(buff, re.KeyStart...)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(re.KeyEnd)))
	buff = append(buff, re.KeyEnd...)
	return buff
}

func (re *RegistrationEntry) deserialize(buff []byte, offset int) int {
	var lev uint32
	lev, offset = common.ReadUint32FromBufferLE(buff, offset)
	re.Level = int(lev)
	var l uint32
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	re.TableID = buff[offset: offset + int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	re.KeyStart = buff[offset: offset + int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	re.KeyEnd = buff[offset: offset + int(l)]
	offset += int(l)
	return offset
}

type RegistrationBatch struct {
	Registrations   []RegistrationEntry
	Deregistrations []RegistrationEntry
}

func (rb *RegistrationBatch) serialize(buff []byte) []byte {
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(rb.Registrations)))
	for _, reg := range rb.Registrations {
		buff = reg.serialize(buff)
	}
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(rb.Deregistrations)))
	for _, dereg := range rb.Deregistrations {
		buff = dereg.serialize(buff)
	}
	return buff
}

func (rb *RegistrationBatch) deserialize(buff []byte, offset int) int {
	var l uint32
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	rb.Registrations = make([]RegistrationEntry, l)
	for i := 0; i < int(l); i++ {
		offset = rb.Registrations[i].deserialize(buff, offset)
	}
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	rb.Deregistrations = make([]RegistrationEntry, l)
	for i := 0; i < int(l); i++ {
		offset = rb.Deregistrations[i].deserialize(buff, offset)
	}
	return offset
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
	version uint64
	levelSegmentEntries map[int][]segmentEntry
}

func (mr *masterRecord) copy() *masterRecord {
	mapCopy := make(map[int][]segmentEntry, len(mr.levelSegmentEntries))
	for k, v := range mr.levelSegmentEntries {
		// TODO make sure v is really copied
		mapCopy[k] = v
	}
	return &masterRecord{
		format:              mr.format,
		version:             mr.version,
		levelSegmentEntries: mapCopy,
	}
}

func (mr *masterRecord) serialize(buff []byte) []byte {
	buff = append(buff, mr.format)
	buff = common.AppendUint64ToBufferLE(buff, mr.version)
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
	mr.version, offset = common.ReadUint64FromBufferLE(buff, offset)
	var nl uint32
	nl, offset = common.ReadUint32FromBufferLE(buff, offset)
	mr.levelSegmentEntries = make(map[int][]segmentEntry, nl)
	for i := 0; i < int(nl); i++ {
		var level, numEntries uint32
		level, offset = common.ReadUint32FromBufferLE(buff, offset)
		numEntries, offset = common.ReadUint32FromBufferLE(buff, offset)
		segEntries := make([]segmentEntry, numEntries)
		for j := 0; j < int(numEntries); j++ {
			segEntry := segmentEntry{}
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

// Validate checks the registry is sound - no overlapping keys in L > 0 etc
func (r *registry) Validate(validateTables bool) error {
	lse := r.masterRecord.levelSegmentEntries
	for level, segentries := range lse {
		if level == 0 {
			for i, segEntry := range segentries {
				if err := r.validateSegment(segEntry, level, i, validateTables); err != nil {
					return err
				}
			}
		} else {
			for i, segEntry := range segentries {
				if i > 0 {
					if bytes.Compare(segentries[i - 1].rangeEnd, segEntry.rangeStart) >= 0 {
						return errors.Errorf("inconsistency. level %d segment entry %d overlapping range with previous", level, i)
					}
				}
				if i < len(segentries) -1 {
					if bytes.Compare(segentries[i + 1].rangeStart, segEntry.rangeEnd) <= 0 {
						return errors.Errorf("inconsistency. level %d segment entry %d overlapping range with next", level, i)
					}
				}
				if err := r.validateSegment(segEntry, level, i, validateTables); err != nil {
					return err
				}
			}
		}
	}
	// There should be no empty levels after the first non empty level
	firstNonEmptyLevel := 0
	for {
		_, ok := lse[firstNonEmptyLevel]
		if ok {
			break
		}
		firstNonEmptyLevel++
	}
	numLevels := len(lse)
	for i := firstNonEmptyLevel; i < firstNonEmptyLevel + numLevels; i++ {
		_, ok := lse[i]
		if !ok {
			return errors.Errorf("empty level %d", i)
		}
	}
	return nil
}

func (r *registry) validateSegment(segEntry segmentEntry, level int, segEntryIndex int, validateTables bool) error {
	seg, err := r.getSegment(segEntry.segmentID)
	if err != nil {
		return err
	}
	if len(seg.tableEntries) == 0 {
		return errors.Errorf("inconsistency. level %d. segment %v has zero table entries", level, segEntry.segmentID)
	}
	if !bytes.Equal(segEntry.rangeStart, seg.tableEntries[0].rangeStart) {
		return errors.Errorf("inconsistency. level %d. segment entry %d rangeStart does not match rangeStart of first table entry", level, segEntryIndex)
	}
	if !bytes.Equal(segEntry.rangeEnd, seg.tableEntries[len(seg.tableEntries) - 1].rangeEnd) {
		return errors.Errorf("inconsistency. level %d. segment entry %d rangeEnd does not match rangeEnd of first table entry", level, segEntryIndex)
	}
	for i, te := range seg.tableEntries {
		if i > 0 {
			if bytes.Compare(seg.tableEntries[i - 1].rangeEnd, te.rangeStart) >= 0 {
				return errors.Errorf("inconsistency. segment %v, table entry %d has overlap with previous", segEntry.segmentID, i)
			}
		}
		if i < len(seg.tableEntries) -1 {
			if bytes.Compare(seg.tableEntries[i + 1].rangeStart, te.rangeEnd) <= 0 {
				return errors.Errorf("inconsistency. segment %v, table entry %d has overlap with next", segEntry.segmentID, i)
			}
		}
		if validateTables {
			if err := r.validateTable(te); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *registry) validateTable(te *tableEntry) error {
	buff, err := r.cloudStore.Get(te.ssTableID)
	if err != nil {
		return err
	}
	if buff == nil {
		return errors.Errorf("cannot find sstable %v", te.ssTableID)
	}
	table := &SSTable{}
	table.Deserialize(buff, 0)
	iter, err := table.NewIterator(table.CommonPrefix(), nil)
	if err != nil {
		return err
	}
	first := true
	var prevKey []byte
	for iter.IsValid() {
		curr := iter.Current()
		if first {
			if !bytes.Equal(te.rangeStart, curr.Key) {
				return errors.Errorf("table %v range start does not match first entry key", te.ssTableID)
			}
			first = false
		}
		if prevKey != nil {
			if bytes.Compare(prevKey, curr.Key) >= 0 {
				return errors.Errorf("table %v keys out of order", te.ssTableID)
			}
		}
		prevKey = curr.Key
	}
	if prevKey == nil {
		return errors.Errorf("table %v has no entries", te.ssTableID)
	}
	if !bytes.Equal(te.rangeEnd, prevKey) {
		return errors.Errorf("table %v range end does not match last entry key", te.ssTableID)
	}
	return nil
}
