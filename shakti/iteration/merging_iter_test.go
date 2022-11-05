package iteration

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMergingIteratorNoDups(t *testing.T) {
	iter1 := createIter(2, 2, 5, 5, 9, 9, 11, 11, 12, 12)
	iter2 := createIter(0, 0, 3, 3, 7, 7, 10, 10, 13, 13)
	iter3 := createIter(4, 4, 6, 6, 8, 8, 14, 14, 18, 18)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14, 18, 18)
}

func TestMergingIteratorDupKeys(t *testing.T) {
	iter1 := createIter(2, 20, 5, 50, 9, 90, 11, 110, 12, 120)
	iter2 := createIter(0, 0, 3, 300, 5, 500, 13, 1300, 14, 1400)
	iter3 := createIter(4, 4000, 5, 5000, 8, 8000, 14, 14000, 18, 18000)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 2, 20, 3, 300, 4, 4000, 5, 50, 8, 8000, 9, 90, 11, 110, 12, 120, 13, 1300, 14, 1400, 18, 18000)
}

func TestMergingIteratorTombstonesDoNotPreserve(t *testing.T) {
	iter1 := createIter(2, -1, 5, -1, 9, 90, 11, -1, 12, 120)
	iter2 := createIter(0, 0, 3, 300, 5, 500, 13, 1300, 14, -1)
	iter3 := createIter(4, -1, 5, 5000, 8, 8000, 14, 14000, 18, 18000)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 3, 300, 8, 8000, 9, 90, 12, 120, 13, 1300, 18, 18000)
}

func TestMergingIteratorTombstonesPreserve(t *testing.T) {
	iter1 := createIter(2, -1, 5, -1, 9, 90, 11, -1, 12, 120)
	iter2 := createIter(0, 0, 3, 300, 5, 500, 13, 1300, 14, -1)
	iter3 := createIter(4, -1, 5, 5000, 8, 8000, 14, 14000, 18, 18000)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, true)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 2, -1, 3, 300, 4, -1, 5, -1, 8, 8000, 9, 90, 11, -1, 12, 120, 13, 1300, 14, -1, 18, 18000)
}

func TestMergingIteratorPutThenTombstonesLater(t *testing.T) {
	// Tests the case where there's a put, then a delete, then a put in iterators
	iter1 := createIter(2, 20, 5, 50, 9, 90, 11, 110, 12, 120)
	iter2 := createIter(0, 0, 4, -1, 5, 500, 9, -1, 13, 1300, 14, -1)
	iter3 := createIter(4, 4000, 5, 5000, 8, 8000, 9, 99, 14, 14000, 18, 18000)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false)
	require.NoError(t, err)
	expectEntries(t, mi, 0, 0, 2, 20, 5, 50, 8, 8000, 9, 90, 11, 110, 12, 120, 13, 1300, 18, 18000)
}

func TestMergingIteratorTombstonesAfterNonTombstoneEntry(t *testing.T) {
	// Tests the case where there is a non tombstone before a tombstone in the iters (9, 90 before 4, -1)
	iter1 := createIter(9, 90)
	iter2 := createIter(4, -1, 5, 500)
	iter3 := createIter(4, 4000, 5, 5000, 8, 8000)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false)
	require.NoError(t, err)
	expectEntries(t, mi, 5, 500, 8, 8000, 9, 90)
}

func TestMergingIteratorOneIterator(t *testing.T) {
	iter1 := createIter(2, 20, 5, 50, 9, 90, 11, 110, 12, 120)
	iters := []Iterator{iter1}
	mi, err := NewMergingIterator(iters, false)
	require.NoError(t, err)
	expectEntries(t, mi, 2, 20, 5, 50, 9, 90, 11, 110, 12, 120)
}

func TestMergingIteratorTwoIteratorsOneSmall(t *testing.T) {
	iter1 := createIter(1, 1, 9, 9)
	iter2 := createIter(2, 20, 5, 50, 9, 90, 11, 110, 12, 120)
	iters := []Iterator{iter1, iter2}
	mi, err := NewMergingIterator(iters, false)
	require.NoError(t, err)
	expectEntries(t, mi, 1, 1, 2, 20, 5, 50, 9, 9, 11, 110, 12, 120)
}

func TestMergingIteratorValidReturnChanges(t *testing.T) {
	// We need to support iterators changing the rersult of IsValid() from false to true on
	// subsequent calls. E.g. this can happen with MemTableIterator as new data arrives between the two calls
	iter1 := createIter(5, 50)
	iter2 := createIter(2, 20)
	iter3 := createIter(3, 30)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false)
	require.NoError(t, err)

	requireIterValid(t, mi, true)
	expectEntry(t, mi, 2, 20)
	err = mi.Next()
	require.NoError(t, err)
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 3, 30)

	// Force iter1 to return not valid
	iter1.SetValidOverride(false)

	err = mi.Next()
	require.NoError(t, err)
	requireIterValid(t, mi, false)

	// Make it valid again
	iter1.UnsetValidOverride()
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 5, 50)

	err = mi.Next()
	require.NoError(t, err)
	requireIterValid(t, mi, false)
}

func TestMergingIteratorValidReturnChangesWithTombstone(t *testing.T) {
	// We need to support iterators changing the rersult of IsValid() from false to true on
	// subsequent calls. E.g. this can happen with MemTableIterator as new data arrives between the two calls
	// Here iterator changes from not valid to valid and reveals a tombstone which deletes an entry from a later iterator
	iter1 := createIter(4, -1)
	iter2 := createIter(2, 20)
	iter3 := createIter(3, 30, 4, 40)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false)
	require.NoError(t, err)

	requireIterValid(t, mi, true)
	expectEntry(t, mi, 2, 20)
	err = mi.Next()
	require.NoError(t, err)
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 3, 30)

	// Force iter1 to return not valid
	iter1.SetValidOverride(false)

	err = mi.Next()
	require.NoError(t, err)
	requireIterValid(t, mi, true)

	// Make it valid again
	iter1.UnsetValidOverride()
	requireIterValid(t, mi, false)
}

func TestMergingIteratorIsValidCurrDoesntAdvanceCursor(t *testing.T) {
	// Make sure that calling IsValid() or Curr() multiple times doesn't advance the cursor
	iter1 := createIter(2, 2)
	iter2 := createIter(0, 0)
	iter3 := createIter(4, 4)
	iters := []Iterator{iter1, iter2, iter3}
	mi, err := NewMergingIterator(iters, false)
	require.NoError(t, err)

	requireIterValid(t, mi, true)
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 0, 0)
	expectEntry(t, mi, 0, 0)
	err = mi.Next()
	require.NoError(t, err)

	requireIterValid(t, mi, true)
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 2, 2)
	expectEntry(t, mi, 2, 2)
	err = mi.Next()
	require.NoError(t, err)

	requireIterValid(t, mi, true)
	requireIterValid(t, mi, true)
	expectEntry(t, mi, 4, 4)
	expectEntry(t, mi, 4, 4)
	err = mi.Next()
	require.NoError(t, err)

	requireIterValid(t, mi, false)
	requireIterValid(t, mi, false)
}

/*
TODO!! Test prepending!!
*/

func expectEntry(t *testing.T, iter Iterator, expKey int, expVal int) {
	t.Helper()
	curr := iter.Current()
	ekey := fmt.Sprintf("key-%010d", expKey)
	require.Equal(t, ekey, string(curr.Key))
	evalue := fmt.Sprintf("value-%010d", expVal)
	require.Equal(t, evalue, string(curr.Value))
}

func expectEntries(t *testing.T, iter Iterator, expected ...int) {
	t.Helper()
	for i := 0; i < len(expected); i++ {
		expKey := expected[i]
		i++
		expVal := expected[i]
		requireIterValid(t, iter, true)
		curr := iter.Current()
		ekey := fmt.Sprintf("key-%010d", expKey)
		log.Printf("got key:%s value:%s", string(curr.Key), string(curr.Value))
		require.Equal(t, ekey, string(curr.Key))
		if expVal != -1 {
			evalue := fmt.Sprintf("value-%010d", expVal)
			require.Equal(t, evalue, string(curr.Value))
		} else {
			require.Nil(t, curr.Value)
		}
		err := iter.Next()
		require.NoError(t, err)
	}
	requireIterValid(t, iter, false)
}

func createIter(vals ...int) *StaticIterator {
	gi := &StaticIterator{}
	for i := 0; i < len(vals); i++ {
		k := vals[i]
		i++
		v := vals[i]
		key := fmt.Sprintf("key-%010d", k)
		if v == -1 {
			gi.AddKV([]byte(key), nil)
		} else {
			value := fmt.Sprintf("value-%010d", v)
			gi.AddKVAsString(key, value)
		}

	}
	return gi
}

func requireIterValid(t *testing.T, iter Iterator, valid bool) {
	t.Helper()
	v, err := iter.IsValid()
	require.NoError(t, err)
	require.Equal(t, valid, v)
}
