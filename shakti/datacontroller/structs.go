package datacontroller

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/shakti/cmn"
	"github.com/squareup/pranadb/shakti/sst"
)

type RegistrationEntry struct {
	Level            int
	TableID          sst.SSTableID
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
	re.TableID = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	re.KeyStart = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	re.KeyEnd = buff[offset : offset+int(l)]
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
	format       byte // Placeholder to allow us to change format later on
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
	ssTableID  sst.SSTableID
	rangeStart []byte
	rangeEnd   []byte
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
	te.ssTableID = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	te.rangeStart = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	te.rangeEnd = buff[offset : offset+int(l)]
	offset += int(l)
	return offset
}

type segmentEntry struct {
	format     byte // Placeholder to allow us to change format later on, e.g. add bloom filter in the future
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
	se.segmentID = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	se.rangeStart = buff[offset : offset+int(l)]
	offset += int(l)
	l, offset = common.ReadUint32FromBufferLE(buff, offset)
	se.rangeEnd = buff[offset : offset+int(l)]
	offset += int(l)
	return offset
}

type masterRecord struct {
	format              cmn.MetadataFormat
	version             uint64
	levelSegmentEntries map[int][]segmentEntry
}

func (mr *masterRecord) copy() *masterRecord {
	mapCopy := make(map[int][]segmentEntry, len(mr.levelSegmentEntries))
	for k, v := range mr.levelSegmentEntries {
		// TODO make sure v is really copied - FIXME!!
		mapCopy[k] = v
	}
	return &masterRecord{
		format:              mr.format,
		version:             mr.version,
		levelSegmentEntries: mapCopy,
	}
}

func (mr *masterRecord) serialize(buff []byte) []byte {
	buff = append(buff, byte(mr.format))
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
	mr.format = cmn.MetadataFormat(buff[offset])
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
