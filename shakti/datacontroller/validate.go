package datacontroller

import (
	"bytes"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/shakti/sst"
)

// Validate checks the controller is sound - no overlapping keys in L > 0 etc
func (c *controller) Validate(validateTables bool) error {
	lse := c.masterRecord.levelSegmentEntries
	if len(lse) == 0 {
		return nil
	}
	for level, segentries := range lse {
		if level == 0 {
			for _, segEntry := range segentries {
				if err := c.validateSegment(segEntry, level, validateTables); err != nil {
					return err
				}
			}
		} else {
			for i, segEntry := range segentries {
				if i > 0 {
					if bytes.Compare(segentries[i-1].rangeEnd, segEntry.rangeStart) >= 0 {
						return errors.Errorf("inconsistency. level %d segment entry %d overlapping range with previous", level, i)
					}
				}
				if i < len(segentries)-1 {
					if bytes.Compare(segentries[i+1].rangeStart, segEntry.rangeEnd) <= 0 {
						return errors.Errorf("inconsistency. level %d segment entry %d overlapping range with next", level, i)
					}
				}
				if err := c.validateSegment(segEntry, level, validateTables); err != nil {
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
	for i := firstNonEmptyLevel; i < firstNonEmptyLevel+numLevels; i++ {
		_, ok := lse[i]
		if !ok {
			return errors.Errorf("empty level %d", i)
		}
	}
	return nil
}

func (c *controller) validateSegment(segEntry segmentEntry, level int, validateTables bool) error {
	seg, err := c.getSegment(segEntry.segmentID)
	if err != nil {
		return err
	}
	if len(seg.tableEntries) == 0 {
		return errors.Errorf("inconsistency. level %d. segment %v has zero table entries", level, segEntry.segmentID)
	}
	if len(seg.tableEntries) > c.conf.MaxRegistrySegmentTableEntries {
		return errors.Errorf("inconsistency. level %d. segment %v has > %d table entries", level, segEntry.segmentID, c.conf.MaxRegistrySegmentTableEntries)
	}
	var smallestKey, largestKey []byte
	for i, te := range seg.tableEntries {
		if smallestKey == nil || bytes.Compare(te.rangeStart, smallestKey) < 0 {
			smallestKey = te.rangeStart
		}
		if largestKey == nil || bytes.Compare(te.rangeEnd, largestKey) > 0 {
			largestKey = te.rangeEnd
		}
		// L0 segment has overlap
		if level > 0 {
			if i > 0 {
				if bytes.Compare(seg.tableEntries[i-1].rangeEnd, te.rangeStart) >= 0 {
					return errors.Errorf("inconsistency. segment %v, table entry %d has overlap with previous", segEntry.segmentID, i)
				}
			}
			if i < len(seg.tableEntries)-1 {
				if bytes.Compare(seg.tableEntries[i+1].rangeStart, te.rangeEnd) <= 0 {
					return errors.Errorf("inconsistency. segment %v, table entry %d has overlap with next", segEntry.segmentID, i)
				}
			}
		}
		if validateTables {
			if err := c.validateTable(te); err != nil {
				return err
			}
		}
	}
	if !bytes.Equal(smallestKey, segEntry.rangeStart) {
		return errors.Errorf("inconsistency. segment %v, smallest table entry does not match rangeStart for the segment", segEntry.segmentID)
	}
	if !bytes.Equal(largestKey, segEntry.rangeEnd) {
		return errors.Errorf("inconsistency. segment %v, largest table entry does not match rangeEnd for the segment", segEntry.segmentID)
	}

	return nil
}

func (c *controller) validateTable(te *tableEntry) error {
	buff, err := c.cloudStore.Get(te.ssTableID)
	if err != nil {
		return err
	}
	if buff == nil {
		return errors.Errorf("cannot find sstable %v", te.ssTableID)
	}
	table := &sst.SSTable{}
	table.Deserialize(buff, 0)
	iter, err := table.NewIterator(table.CommonPrefix(), nil)
	if err != nil {
		return err
	}
	first := true
	var prevKey []byte
	for {
		v, err := iter.IsValid()
		if err != nil {
			return err
		}
		if !v {
			break
		}
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
