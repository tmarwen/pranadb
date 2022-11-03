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

func expectEntries(t *testing.T, iter Iterator, expected ...int) {
	t.Helper()
	for i := 0; i < len(expected); i++ {
		expKey := expected[i]
		i++
		expVal := expected[i]
		require.True(t, iter.IsValid())
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
	require.False(t, iter.IsValid())
}

func createIter(vals ...int) Iterator {
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
