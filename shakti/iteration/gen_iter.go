package iteration

import (
	"github.com/squareup/pranadb/shakti/cmn"
)

// StaticIterator is used in tests only
type StaticIterator struct {
	kvs                []cmn.KV
	pos                int
	hasValidOverride   bool
	validOverRideValue bool
}

func (s *StaticIterator) SetValidOverride(valid bool) {
	s.hasValidOverride = true
	s.validOverRideValue = valid
}

func (s *StaticIterator) UnsetValidOverride() {
	s.hasValidOverride = false
}

func (s *StaticIterator) AddKVAsString(k string, v string) {
	s.kvs = append(s.kvs, cmn.KV{
		Key:   []byte(k),
		Value: []byte(v),
	})
}

func (s *StaticIterator) AddKV(k []byte, v []byte) {
	s.kvs = append(s.kvs, cmn.KV{
		Key:   k,
		Value: v,
	})
}

func (s *StaticIterator) Current() cmn.KV {
	if s.pos == -1 {
		return cmn.KV{}
	}
	return s.kvs[s.pos]
}

func (s *StaticIterator) Next() error {
	s.pos++
	if s.pos == len(s.kvs) {
		s.pos = -1
	}
	return nil
}

func (s *StaticIterator) IsValid() (bool, error) {
	if s.hasValidOverride {
		return s.validOverRideValue, nil
	}
	return s.pos != -1, nil
}

func (s *StaticIterator) Close() error {
	return nil
}
