package shakti

import (
	"testing"
)

func TestChainingIterator(t *testing.T) {
	iter1 := createIter(1, 1, 3, 3, 7, 7, 8, 8, 9, 9)
	iter2 := createIter(12, 12, 13, 13, 17, 17)
	iter3 := createIter(18, 18, 20, 20, 21, 21, 25, 25, 26, 26)
	ci := NewChainingIterator([]Iterator{iter1, iter2, iter3})
	expectEntries(t, ci, 1, 1, 3, 3, 7, 7, 8, 8, 9, 9, 12, 12, 13, 13, 17, 17, 18, 18, 20, 20, 21, 21, 25, 25, 26, 26)
}
