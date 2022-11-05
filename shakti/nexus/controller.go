package nexus

import (
	"bufio"
	"bytes"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/shakti/cloudstore"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/sst"
	"io"
	"os"
	"sync"
)

type Controller interface {
	GetTableIDsForRange(keyStart []byte, keyEnd []byte, maxTablesPerLevel int) (OverlappingTableIDs, error)
	ApplyChanges(registrationBatch RegistrationBatch) error
	SetLeader() error
	Start() error
	Stop() error
	Validate(validateTables bool) error
	Flush() (int, int, error)
}

type NonoverlappingTableIDs []sst.SSTableID
type OverlappingTableIDs []NonoverlappingTableIDs

type Replicator interface {
	ReplicateMessage(message []byte) error
}

type Replica interface {
	ReceiveReplicationMessage(message []byte) error
}

type controller struct {
	lock                           sync.RWMutex
	format                         cmn.MetadataFormat
	cloudStore                     cloudstore.Store
	conf                           Conf
	segmentCache                   sync.Map // TODO make this a LRU cache
	masterRecord                   *masterRecord
	segmentsToAdd                  map[string]*segment
	segmentsToDelete               map[string]struct{}
	masterRecordBufferSizeEstimate int
	segmentBufferSizeEstimate      int
	replicator                     Replicator
	logFile                        *os.File
	receivedFlush                  bool
	leader                         bool
}

// NewController TODO add total tables counter and keep track of tables per segment average so we can monitor fragmentation
func NewController(conf Conf, cloudStore cloudstore.Store, replicator Replicator) Controller {
	return newController(conf, cloudStore, replicator)
}

func newController(conf Conf, cloudStore cloudstore.Store, replicator Replicator) *controller {
	return &controller{
		format:           conf.RegistryFormat,
		cloudStore:       cloudStore,
		conf:             conf,
		replicator:       replicator,
		segmentsToAdd:    map[string]*segment{},
		segmentsToDelete: map[string]struct{}{},
	}
}

func (c *controller) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	buff, err := c.cloudStore.Get([]byte(c.conf.MasterRegistryRecordID))
	if err != nil {
		return err
	}
	if buff != nil {
		mr := &masterRecord{}
		mr.deserialize(buff, 0)
		c.masterRecord = mr
	} else {
		c.masterRecord = &masterRecord{
			format:              c.conf.RegistryFormat,
			levelSegmentEntries: map[int][]segmentEntry{},
		}
		buff := c.masterRecord.serialize(nil)
		if err := c.cloudStore.Add([]byte(c.conf.MasterRegistryRecordID), buff); err != nil {
			return err
		}
		log.Infof("created prana controller with id %s", c.conf.MasterRegistryRecordID)
	}
	return nil
}

func (c *controller) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.masterRecord = nil
	return nil
}

func (c *controller) updateMasterRecordBufferSizeEstimate(buffSize int) {
	if buffSize > c.masterRecordBufferSizeEstimate {
		c.masterRecordBufferSizeEstimate = int(float64(buffSize) * 1.05)
	}
}

func (c *controller) updateSegmentBufferSizeEstimate(buffSize int) {
	if buffSize > c.segmentBufferSizeEstimate {
		c.segmentBufferSizeEstimate = int(float64(buffSize) * 1.05)
	}
}

func (c *controller) SetLeader() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.leader {
		return errors.New("already leader")
	}
	c.leader = true
	return nil
}

func (c *controller) getSegment(segmentID []byte) (*segment, error) {
	skey := common.ByteSliceToStringZeroCopy(segmentID)
	seg, ok := c.segmentCache.Load(skey)
	if ok {
		return seg.(*segment), nil
	}
	buff, err := c.cloudStore.Get(segmentID)
	if err != nil {
		return nil, err
	}
	if buff == nil {
		return nil, errors.Errorf("cannot find segment with id %d", segmentID)
	}
	segment := &segment{}
	segment.deserialize(buff)
	c.segmentCache.Store(skey, segment)
	return segment, nil
}

func (c *controller) findTableIDsWithOverlapInSegment(seg *segment, keyStart []byte, keyEnd []byte) []sst.SSTableID {
	var tableIDs []sst.SSTableID
	for _, tabEntry := range seg.tableEntries {
		if hasOverlap(keyStart, keyEnd, tabEntry.rangeStart, tabEntry.rangeEnd) {
			tableIDs = append(tableIDs, tabEntry.ssTableID)
		}
	}
	return tableIDs
}

func (c *controller) getMasterRecord() *masterRecord {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.masterRecord.copy()
}

// TODO implement maxTablesPerLevel!!
func (c *controller) GetTableIDsForRange(keyStart []byte, keyEnd []byte, maxTablesPerLevel int) (OverlappingTableIDs, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if !c.leader {
		return nil, errors.New("not leader")
	}
	var overlapping OverlappingTableIDs
	level := 0
	levelCount := 0
	nl := len(c.masterRecord.levelSegmentEntries)
	for ; levelCount < nl; level++ {
		segmentEntries, ok := c.masterRecord.levelSegmentEntries[level]
		if !ok {
			continue
		}
		var tableIDs []sst.SSTableID
		for _, segEntry := range segmentEntries {
			if hasOverlap(keyStart, keyEnd, segEntry.rangeStart, segEntry.rangeEnd) {
				seg, err := c.getSegment(segEntry.segmentID)
				if err != nil {
					return nil, err
				}
				// TODO For L0 where there is overlap and a single segment, we could use an interval tree
				// TODO for L > 0 where there is no overlap we can use binary search to locate the segments
				// and also to locate the table ids in the segment
				tableIDs = append(tableIDs, c.findTableIDsWithOverlapInSegment(seg, keyStart, keyEnd)...)
			}
		}
		if level == 0 {
			// Level 0 is overlapping
			for _, tableID := range tableIDs {
				overlapping = append(overlapping, []sst.SSTableID{tableID})
			}
		} else if tableIDs != nil {
			// Other levels are non overlapping
			overlapping = append(overlapping, tableIDs)
		}
		levelCount++
	}
	return overlapping, nil
}

func (c *controller) ApplyChanges(registrationBatch RegistrationBatch) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.leader {
		return errors.New("not leader")
	}
	if err := c.replicateBatch(c.masterRecord.version, &registrationBatch); err != nil {
		return err
	}
	// We must process the deregistrations before registrations or we can temporarily have overlapping keys
	if err := c.applyDeregistrations(registrationBatch.Deregistrations); err != nil {
		return err
	}
	if err := c.applyRegistrations(registrationBatch.Registrations); err != nil {
		return err
	}
	c.masterRecord.version++
	return nil
}

func (c *controller) applyDeregistrations(deregistrations []RegistrationEntry) error { //nolint:gocyclo
	for _, deregistration := range deregistrations {
		segmentEntries, ok := c.masterRecord.levelSegmentEntries[deregistration.Level]
		if !ok {
			return errors.Errorf("cannot find level %d for deregistration", deregistration.Level)
		}
		var segEntry *segmentEntry
		found := -1
		if deregistration.Level == 0 {
			if len(segmentEntries) == 0 {
				return errors.Error("no segment for level 0")
			}
			segEntry = &segmentEntries[0]
			found = 0
		} else {
			// Find which segment entry the table is in
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
			segEntry = &segmentEntries[found]
		}

		// Load the segment
		seg, err := c.getSegment(segEntry.segmentID)
		if err != nil {
			return err
		}

		// Find the table entry in the segment entry
		pos := -1
		// TODO for L > 0 we can do a binary search here based on range start as no overlaps
		// for L = 0 there are overlaps so we can't, but we could use an interval tree
		for i, te := range seg.tableEntries {
			if bytes.Equal(deregistration.TableID, te.ssTableID) {
				pos = i
				break
			}
		}
		if pos == -1 {
			return errors.Error("cannot find table entry in segment")
		}
		newTableEntries := seg.tableEntries[:pos]
		newTableEntries = append(newTableEntries, seg.tableEntries[pos+1:]...)

		// TODO if number of table entries is below a threshold and left or right neighbour
		// has enough space to take the entries, then can merge them

		// Remove the old segment
		c.segmentToRemove(segEntry.segmentID)

		if len(newTableEntries) == 0 {
			// We remove the segment entry - it is empty
			newSegEntries := segmentEntries[:found]
			newSegEntries = append(newSegEntries, segmentEntries[found+1:]...)
			segmentEntries = newSegEntries
		} else {
			newSeg := &segment{
				format:       seg.format,
				tableEntries: newTableEntries,
			}
			// Add the new segment
			id, err := c.segmentToAdd(newSeg)
			if err != nil {
				return err
			}
			var newStart, newEnd []byte
			if deregistration.Level == 0 {
				// Level 0 is not ordered so we need to scan through all of them
				for _, te := range newTableEntries {
					if newStart == nil || bytes.Compare(te.rangeStart, newStart) < 0 {
						newStart = te.rangeStart
					}
					if newEnd == nil || bytes.Compare(te.rangeEnd, newEnd) > 0 {
						newEnd = te.rangeEnd
					}
				}
			} else {
				newStart = newTableEntries[0].rangeStart
				newEnd = newTableEntries[len(newTableEntries)-1].rangeEnd
			}
			newSegEntry := segmentEntry{
				format:     segEntry.format,
				segmentID:  id,
				rangeStart: newStart,
				rangeEnd:   newEnd,
			}
			segmentEntries[found] = newSegEntry
		}
		if len(segmentEntries) == 0 {
			delete(c.masterRecord.levelSegmentEntries, deregistration.Level)
		} else {
			c.masterRecord.levelSegmentEntries[deregistration.Level] = segmentEntries
		}
	}
	return nil
}

func (c *controller) applyRegistrations(registrations []RegistrationEntry) error { //nolint:gocyclo
	for _, registration := range registrations {
		// The new table entry that we're going to add
		tabEntry := &tableEntry{
			ssTableID:  registration.TableID,
			rangeStart: registration.KeyStart,
			rangeEnd:   registration.KeyEnd,
		}
		segmentEntries, ok := c.masterRecord.levelSegmentEntries[registration.Level]
		if registration.Level == 0 {
			// We have overlapping keys in L0 so we just append to the last segment
			var seg *segment
			var segRangeStart, segRangeEnd []byte
			if ok {
				// Segment already exists
				// Level 0 only ever has one segment
				l0SegmentEntry := segmentEntries[0]
				var err error
				seg, err = c.getSegment(l0SegmentEntry.segmentID)
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
				// Delete the old segment
				c.segmentToRemove(l0SegmentEntry.segmentID)
			} else {
				// Create a new segment
				seg = &segment{
					tableEntries: []*tableEntry{tabEntry},
				}
				segRangeStart = registration.KeyStart
				segRangeEnd = registration.KeyEnd
			}

			// Add the new segment
			id, err := c.segmentToAdd(seg)
			if err != nil {
				return err
			}

			// Update the master record
			c.masterRecord.levelSegmentEntries[0] = []segmentEntry{{
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
				if (i == 0 || bytes.Compare(registration.KeyStart, segmentEntries[i-1].rangeEnd) > 0) &&
					(i == len(segmentEntries)-1 || bytes.Compare(registration.KeyEnd, segmentEntries[i+1].rangeStart) < 0) {
					found = i
					break
				}
			}
			//log.Printf("Adding key start %s key end %s", string(registration.KeyStart), string(registration.KeyEnd))
			if len(segmentEntries) > 0 && found == -1 {
				panic("cannot find segment for new table entry")
			}
			if found != -1 {
				//log.Printf("found is %d", found)
				//log.Printf("there are %d segment entries", len(segmentEntries))
				segEntry := segmentEntries[found]
				seg, err := c.getSegment(segEntry.segmentID)
				if err != nil {
					return err
				}
				// TODO binary search??
				// Find the insert point
				insertPoint := -1
				for i, te := range seg.tableEntries {
					if bytes.Compare(registration.KeyEnd, te.rangeStart) < 0 {
						insertPoint = i
						break
					}
				}
				// Insert the new entry in the table entries in the right place
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

				//log.Printf("insert point is %d", insertPoint)

				//seg.tableEntries = newTableEntries

				var nextSegID segmentID
				// Create the new segment(s)
				var newSegs []segment
				lnte := len(newTableEntries)
				//log.Printf("length of new table entries is %d", lnte)
				if lnte > c.conf.MaxRegistrySegmentTableEntries {
					//log.Println("There are two many entries")
					// Too many entries
					// If there is a next segment and it's not full we will merge it into that one otherwise
					// we will create a new segment
					merged := false
					if found < len(segmentEntries)-1 {
						//log.Println("There is a next segment")
						nextSegID = segmentEntries[found+1].segmentID
						nextSeg, err := c.getSegment(nextSegID)
						if err != nil {
							return err
						}
						if len(nextSeg.tableEntries) < c.conf.MaxRegistrySegmentTableEntries {
							//log.Printf("merging into next which has %d entries", len(nextSeg.tableEntries))
							// The next segment has space, merge into that one
							te1 := newTableEntries[:lnte-1]
							te2 := make([]*tableEntry, 0, len(nextSeg.tableEntries)+1)
							te2 = append(te2, newTableEntries[lnte-1])
							te2 = append(te2, nextSeg.tableEntries...)
							newSegs = append(newSegs, segment{format: byte(c.format), tableEntries: te1}, segment{format: byte(c.format), tableEntries: te2})
							merged = true
						} else {
							//log.Println("but it is already full")
							nextSegID = nil
						}
					}
					if !merged {
						//log.Printf("Didn't merge so creating a new segment")
						// We didn't merge into the next one, so create a new segment
						te1 := newTableEntries[:lnte-1]
						te2 := newTableEntries[lnte-1:]
						newSegs = append(newSegs, segment{format: byte(c.format), tableEntries: te1}, segment{format: byte(c.format), tableEntries: te2})
					}
				} else {
					newSegs = []segment{{format: byte(c.format), tableEntries: newTableEntries}}
				}

				// Delete the old segment
				c.segmentToRemove(segEntry.segmentID)
				// Delete the next segment if we replaced that too
				if nextSegID != nil {
					c.segmentToRemove(nextSegID)
				}
				// Store the new segment(s)
				newEntries := make([]segmentEntry, len(newSegs))
				for i, newSeg := range newSegs {
					nseg := newSeg
					id, err := c.segmentToAdd(&nseg)
					if err != nil {
						return err
					}
					newEntries[i] = segmentEntry{
						segmentID:  id,
						rangeStart: newSeg.tableEntries[0].rangeStart,
						rangeEnd:   newSeg.tableEntries[len(newSeg.tableEntries)-1].rangeEnd,
					}
				}

				// Create the new segment entries
				newSegEntries := make([]segmentEntry, 0, len(segmentEntries)-1+len(newSegs))
				newSegEntries = append(newSegEntries, segmentEntries[:found]...)
				newSegEntries = append(newSegEntries, newEntries...)
				pos := found + 1
				if nextSegID != nil {
					// We changed the next segment too so we replace two entries
					pos++
				}
				newSegEntries = append(newSegEntries, segmentEntries[pos:]...)

				//log.Printf("now there are %d segment entries", len(newSegEntries))

				// Update the master record
				c.masterRecord.levelSegmentEntries[registration.Level] = newSegEntries

				//log.Println("New seg entries::::")
				//for _, segEntry := range newSegEntries {
				//	log.Printf("range start %s range end %s", string(segEntry.rangeStart), string(segEntry.rangeEnd))
				//}
			} else {
				// The first segment in the level
				seg := &segment{tableEntries: []*tableEntry{tabEntry}}
				id, err := c.segmentToAdd(seg)
				if err != nil {
					return err
				}
				segEntry := segmentEntry{
					segmentID:  id,
					rangeStart: registration.KeyStart,
					rangeEnd:   registration.KeyEnd,
				}
				c.masterRecord.levelSegmentEntries[registration.Level] = []segmentEntry{segEntry}
			}
		}
	}
	return nil
}

func (c *controller) segmentToAdd(seg *segment) ([]byte, error) {
	id, err := uuid.New().MarshalBinary()
	if err != nil {
		return nil, err
	}
	sid := common.ByteSliceToStringZeroCopy(id)
	// TODO we must pin segments in cache until they are flushed!!
	c.segmentCache.Store(sid, seg)
	c.segmentsToAdd[sid] = seg
	return id, nil
}

func (c *controller) segmentToRemove(segID segmentID) {
	sid := common.ByteSliceToStringZeroCopy(segID)
	c.segmentCache.Delete(sid)
	if _, exists := c.segmentsToAdd[sid]; exists {
		// The seg was created after last flush so just delete it from segmentsToAdd
		delete(c.segmentsToAdd, sid)
	} else {
		c.segmentsToDelete[sid] = struct{}{}
	}
}

func (c *controller) ReceiveReplicationMessage(message []byte) error {
	/*
		replication message is
		flush: 1 byte, 0 = false, true otherwise
		version 8 bytes - (uint64 LE)
		crc32 4 bytes (uint32 LE)
		batch length 4 bytes (uint32 LE)
		batch bytes
	*/

	c.lock.Lock()
	defer c.lock.Unlock()

	flush := true
	if message[0] == 0 {
		flush = false
	}
	if flush {
		// The master record corresponding to all the batches in the log has been stored permanently so we can
		// close the log file and re-open it, truncating it to the beginning.
		// TODO benchmark the close and open to see how long it takes
		if err := c.logFile.Close(); err != nil {
			return err
		}
		if err := c.openOrCreateLogFile(c.conf.LogFileName); err != nil {
			return err
		}
		c.receivedFlush = true
		return nil
	}
	if !c.receivedFlush {
		// We ignore any replicated batches until we have received the first flush
		return nil
	}
	// We append the rest of the message direct to the log, avoiding deserialization/serialization cost
	_, err := c.logFile.Write(message[1:])
	if err != nil {
		return err
	}
	return nil
}

func (c *controller) openOrCreateLogFile(filename string) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	c.logFile = f
	return nil
}

func (c *controller) replicateBatch(version uint64, batch *RegistrationBatch) error {
	buff := make([]byte, 0) // TODO better buff size estimate
	buff = append(buff, 0)  // flush = false
	buff = common.AppendUint64ToBufferLE(buff, version)
	buff = common.AppendUint32ToBufferLE(buff, 0) // CRC32 TODO
	buff = batch.serialize(buff)
	return c.replicator.ReplicateMessage(buff)
}

func (c *controller) recover() (bool, error) {
	// Assumes latest master record has been loaded before this is called
	f, err := os.OpenFile(c.conf.LogFileName, os.O_RDONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	latestVersion := c.masterRecord.version
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

func (c *controller) Flush() (int, int, error) {

	/*
		When we apply a batch we first replicate the batch, then we process the batch on the leader, updating the in-memory
		state. The data is then available to be read, as the batch has been replicated to multiple replicas.

		If a failure occurs the batches will be processed on the new leader before it becomes live and the controller will
		end up in the same state. It doesn't matter if UUIDs of segments are different as these are not exposed to the user
		of the controller.

		During processing of the batch, there will be a set of new segments add and a new version of the master record at
		a particular version. We will add these segments to a map of segments to push and we will take a copy of the master record
		and store that in a member variable.

		Periodically, we will take a copy of the segment to push map and the stored master record copy and reset them.
		We will then push the segments, and push the master record. Then a flush can be replicated to replicas which can clear their batch log up to
		that version of the master record.

		If too many segments to push build up, then we will lock updates and force a flush to prevent the backlog growing too fast
	*/

	c.lock.Lock()
	if len(c.segmentsToAdd) == 0 && len(c.segmentsToDelete) == 0 {
		// No changes since last flush
		c.lock.Unlock()
		return 0, 0, nil
	}
	masterRecordToFlush := c.masterRecord.copy()
	segsToAdd := c.segmentsToAdd
	segsToDelete := c.segmentsToDelete
	c.segmentsToAdd = map[string]*segment{}
	c.segmentsToDelete = map[string]struct{}{}
	c.lock.Unlock()

	// TODO add and delete them in parallel

	// First delete segments
	segsDeleted := len(segsToDelete)
	for sid := range segsToDelete {
		segID := common.StringToByteSliceZeroCopy(sid)
		if err := c.cloudStore.Delete(segID); err != nil {
			return 0, 0, err
		}
	}
	// Then the adds
	segsAdded := len(segsToAdd)
	for sid, seg := range segsToAdd {
		segID := common.StringToByteSliceZeroCopy(sid)
		buff := make([]byte, 0, c.segmentBufferSizeEstimate)
		buff = seg.serialize(buff)
		c.updateSegmentBufferSizeEstimate(len(buff))
		if err := c.cloudStore.Add(segID, buff); err != nil {
			return 0, 0, err
		}
	}
	// Once they've all been added we can flush the master record
	buff := make([]byte, 0, c.masterRecordBufferSizeEstimate)
	buff = masterRecordToFlush.serialize(buff)
	c.updateMasterRecordBufferSizeEstimate(len(buff))
	return segsAdded, segsDeleted, c.cloudStore.Add([]byte(c.conf.MasterRegistryRecordID), buff)
}

func (c *controller) maybeTriggerCompaction() error {
	/*
		Check if too many files in levels, and if so schedule a compaction
		Keep track of compactions already scheduled but not run yet so we don't schedule them more than once

		How do we choose which tables to compact for L > 0?

		In this case tables in the level have no overlapping keys, and are laid out in segments in key order
		We choose a table to compact by selecting the table with the next highest key range to the last one compacted in that
		level, if there is no such table we wrap around to the lowest key ranged table in the level

		Once we have chosen a table we find the set of tables with any key overlap in the next level, these are then sent to the compactor
		which outputs one or more sstables, these are then stored in cloud storage and then applied to controller in a change batch
		which also includes deletion of the old sstable from the previous level and old tables from the higher level.
	*/
	return nil
}

func hasOverlap(keyStart []byte, keyEnd []byte, blockKeyStart []byte, blockKeyEnd []byte) bool {
	// Note! keyStart is inclusive, keyEnd is exclusive
	// controller keyStart and keyEnd are inclusive!
	dontOverlapRight := bytes.Compare(keyStart, blockKeyEnd) > 0                  // Range starts after end of block
	dontOverlapLeft := keyEnd != nil && bytes.Compare(keyEnd, blockKeyStart) <= 0 // Range ends before beginning of block
	dontOverlap := dontOverlapLeft || dontOverlapRight
	return !dontOverlap
}
