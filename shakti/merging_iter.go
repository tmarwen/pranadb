package shakti

import "bytes"

// TODO this can be improved / optimised
// Also, when same key is found multiple tables we need to ignore older ones
// When it's created we call Next()
// We cache the current CK, or if first time this is nil
// In Next we look at all the iters in turn and return the smallest one from left to right which is > CK, if the value
// on the iterator is <= CK we call Next on it before looking at it's value.
// If the value of the KV is nil then it's a tombstone and we skip it
// We then cache this value and simply return it in Current()
// If there is no such value then we set current to nil and is valid = false in this case
type MergingIterator struct {
	iters []Iterator
	current int
}

func (m *MergingIterator) Current() KV {
	var currKey []byte
	if m.current == -1 {
		for i, iter := range m.iters {
			valid := iter.IsValid()
			if valid {
				c := iter.Current()
				if currKey == nil || bytes.Compare(c.Key, currKey) < 0 {
					currKey = c.Key
					m.current = i
				}
			}
		}
	}
	if m.current == -1 {
		return KV{}
	}
	return m.iters[m.current].Current()
}

func (m *MergingIterator) Next() error {
	if m.current == -1 {
		m.Current()
	}
	if m.current != -1 {
		return m.iters[m.current].Next()
	}
	return nil
}

func (m *MergingIterator) IsValid() bool {
	for _, iter := range m.iters {
		valid := iter.IsValid()
		if valid {
			return true
		}
	}
	return false
}

