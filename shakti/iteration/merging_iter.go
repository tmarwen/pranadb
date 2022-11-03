package iteration

import (
	"bytes"
	"github.com/squareup/pranadb/shakti/cmn"
)

type MergingIterator struct {
	iters              []Iterator
	preserveTombstones bool
	current            cmn.KV
	valid              bool
}

func NewMergingIterator(iters []Iterator, preserveTombstones bool) (Iterator, error) {
	mi := &MergingIterator{
		iters:              iters,
		preserveTombstones: preserveTombstones,
	}
	if err := mi.Next(); err != nil {
		return nil, err
	}
	return mi, nil
}

func (m *MergingIterator) Current() cmn.KV {
	if !m.valid {
		return cmn.KV{}
	}
	return m.current
}

func (m *MergingIterator) Next() error {
	repeat := true
	for repeat {
		// Find the smallest key
		var smallestKey []byte
		for _, iter := range m.iters {
			valid := iter.IsValid()
			if valid {
				c := iter.Current()
				if smallestKey == nil || bytes.Compare(c.Key, smallestKey) < 0 {
					smallestKey = c.Key
				}
			}
		}
		if smallestKey == nil {
			m.valid = false
			return nil
		}
		first := true
		// Take the first occurrence left to right of the smallest key as the current entry
		// Move all occurrences of the iters with that key to the next

		for _, iter := range m.iters {
			valid := iter.IsValid()
			if valid {
				c := iter.Current()
				if bytes.Equal(c.Key, smallestKey) {
					if first {
						//  nil value means deleted
						if c.Value != nil || m.preserveTombstones {
							m.current = c
							m.valid = true
							// Not deleted - we will exit the outer loop
							repeat = false
						}
						first = false
					}
					if err := iter.Next(); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (m *MergingIterator) IsValid() bool {
	return m.valid
}
