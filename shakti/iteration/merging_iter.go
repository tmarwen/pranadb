package iteration

import (
	"bytes"
	"github.com/squareup/pranadb/shakti/cmn"
)

type MergingIterator struct {
	iters              []Iterator
	preserveTombstones bool
	current            cmn.KV
	isValidCalled      bool
}

func NewMergingIterator(iters []Iterator, preserveTombstones bool) (*MergingIterator, error) {
	mi := &MergingIterator{
		iters:              iters,
		preserveTombstones: preserveTombstones,
	}
	if err := mi.Next(); err != nil {
		return nil, err
	}
	return mi, nil
}

func (m *MergingIterator) PrependIterator(iter Iterator) error {
	iters := make([]Iterator, 0, len(m.iters)+1)
	iters = append(iters, iter)
	iters = append(iters, m.iters...)
	m.iters = iters
	_, err := m.IsValid()
	return err
}

func (m *MergingIterator) Current() cmn.KV {
	if !m.isValidCalled {
		panic("must call IsValid on each iteration")
	}
	return m.current
}

func (m *MergingIterator) Next() error {
	// Fast forward past the current key
	if m.current.Key != nil {
		for _, iter := range m.iters {
			valid, err := iter.IsValid()
			if err != nil {
				return err
			}
			if valid {
				c := iter.Current()
				if bytes.Equal(c.Key, m.current.Key) {
					if err := iter.Next(); err != nil {
						return err
					}
				}
			}
		}
	}
	m.isValidCalled = false
	return nil
}

func (m *MergingIterator) IsValid() (bool, error) {
	repeat := true
	m.current = cmn.KV{}
	var prevSmallestKey []byte
	for repeat {
		var smallestKey []byte
		for _, iter := range m.iters {
			valid, err := iter.IsValid()
			if err != nil {
				return false, err
			}
			if valid {
				c := iter.Current()
				if prevSmallestKey != nil && bytes.Equal(c.Key, prevSmallestKey) {
					// Fast forward past deleted entries
					if err := iter.Next(); err != nil {
						return false, err
					}
					valid, err = iter.IsValid()
					if err != nil {
						return false, err
					}
					if valid {
						c = iter.Current()
					}
				}
				if valid {
					if smallestKey == nil || bytes.Compare(c.Key, smallestKey) < 0 {
						smallestKey = c.Key
						if c.Value != nil || m.preserveTombstones {
							m.current = c
							repeat = false
						} else {
							repeat = true
						}
						// For a tombstone - we will go around the loop again, and prevSmallest key (which will be
						// this key) will be skipped
					}
				}
			}
		}
		if smallestKey == nil {
			// Nothing valid
			break
		}
		prevSmallestKey = smallestKey
	}
	m.isValidCalled = true
	return m.current.Key != nil, nil
}

func (m *MergingIterator) Close() error {
	return nil
}
