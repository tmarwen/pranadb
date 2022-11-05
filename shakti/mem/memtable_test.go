package mem

import (
	"fmt"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/shakti/iteration"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMTIteratorPicksUpNewRecords(t *testing.T) {
	memTable := NewMemtable(1024 * 1024)

	iter := memTable.NewIterator(nil, nil)
	requireIterValid(t, iter, false)

	addToMemtable(t, memTable, "key0", "val0")
	addToMemtable(t, memTable, "key1", "val1")
	addToMemtable(t, memTable, "key2", "val2")
	addToMemtable(t, memTable, "key3", "val3")
	addToMemtable(t, memTable, "key4", "val4")

	// Iter won't see elements added after writeIter's reached the end
	requireIterValid(t, iter, true)

	for i := 0; i < 5; i++ {
		requireIterValid(t, iter, true)
		curr := iter.Current()
		require.Equal(t, fmt.Sprintf("key%d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%d", i), string(curr.Value))
		err := iter.Next()
		require.NoError(t, err)
	}

	// Call it twice to make sure isValid doesn't change state
	requireIterValid(t, iter, false)
	requireIterValid(t, iter, false)
	addToMemtable(t, memTable, "key5", "val5")
	requireIterValid(t, iter, true)
	requireIterValid(t, iter, true)

	curr := iter.Current()
	require.Equal(t, "key5", string(curr.Key))
	require.Equal(t, "val5", string(curr.Value))
	err := iter.Next()
	require.NoError(t, err)
	requireIterValid(t, iter, false)
}

func TestMTIteratorAddNonKeyOrder(t *testing.T) {
	memTable := NewMemtable(1024 * 1024)

	addToMemtable(t, memTable, "key0", "val0")
	addToMemtable(t, memTable, "key1", "val1")
	addToMemtable(t, memTable, "key2", "val2")
	addToMemtable(t, memTable, "key3", "val3")
	addToMemtable(t, memTable, "key4", "val4")

	iter := memTable.NewIterator(nil, nil)
	requireIterValid(t, iter, true)
	for i := 0; i < 5; i++ {
		requireIterValid(t, iter, true)
		curr := iter.Current()
		require.Equal(t, fmt.Sprintf("key%d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%d", i), string(curr.Value))
		err := iter.Next()
		require.NoError(t, err)
	}
}

func TestMTIteratorAddInNonKeyOrder(t *testing.T) {
	memTable := NewMemtable(1024 * 1024)

	addToMemtable(t, memTable, "key3", "val3")
	addToMemtable(t, memTable, "key2", "val2")
	addToMemtable(t, memTable, "key1", "val1")
	addToMemtable(t, memTable, "key0", "val0")
	addToMemtable(t, memTable, "key4", "val4")

	iter := memTable.NewIterator(nil, nil)
	requireIterValid(t, iter, true)
	for i := 0; i < 5; i++ {
		requireIterValid(t, iter, true)
		curr := iter.Current()
		require.Equal(t, fmt.Sprintf("key%d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%d", i), string(curr.Value))
		err := iter.Next()
		require.NoError(t, err)
	}
}

func TestMTIteratorOverwriteKeys(t *testing.T) {
	memTable := NewMemtable(1024 * 1024)

	addToMemtable(t, memTable, "key3", "val3")
	addToMemtable(t, memTable, "key2", "val2")
	addToMemtable(t, memTable, "key1", "val1")
	addToMemtable(t, memTable, "key0", "val0")
	addToMemtable(t, memTable, "key2", "val5")
	addToMemtable(t, memTable, "key4", "val4")
	addToMemtable(t, memTable, "key0", "val6")

	iter := memTable.NewIterator(nil, nil)
	requireIterValid(t, iter, true)
	for i := 0; i < 5; i++ {
		requireIterValid(t, iter, true)
		curr := iter.Current()
		j := i
		if i == 0 {
			j = 6
		} else if i == 2 {
			j = 5
		}
		require.Equal(t, fmt.Sprintf("key%d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%d", j), string(curr.Value))
		err := iter.Next()
		require.NoError(t, err)
	}
}

func TestMTIteratorTombstones(t *testing.T) {
	memTable := NewMemtable(1024 * 1024)

	addToMemtable(t, memTable, "key3", "val3")
	addToMemtable(t, memTable, "key2", "val2")
	addToMemtableWithByteSlice(t, memTable, "key1", nil)
	addToMemtable(t, memTable, "key0", "val0")
	addToMemtableWithByteSlice(t, memTable, "key4", nil)

	iter := memTable.NewIterator(nil, nil)
	requireIterValid(t, iter, true)
	for i := 0; i < 5; i++ {
		requireIterValid(t, iter, true)
		curr := iter.Current()
		require.Equal(t, fmt.Sprintf("key%d", i), string(curr.Key))
		if i == 1 || i == 4 {
			require.Nil(t, curr.Value)
		} else {
			require.Equal(t, fmt.Sprintf("val%d", i), string(curr.Value))
		}
		err := iter.Next()
		require.NoError(t, err)
	}
}

func TestMTIteratorMultipleIterators(t *testing.T) {
	memTable := NewMemtable(1024 * 1024)

	numEntries := 1000

	go func() {
		for i := 0; i < numEntries; i++ {
			addToMemtable(t, memTable, fmt.Sprintf("key%010d", i), fmt.Sprintf("val%010d", i))
			time.Sleep(1 * time.Millisecond)
		}
	}()

	numIters := 10

	chans := make([]chan error, numIters)
	for i := 0; i < numIters; i++ {
		ch := make(chan error)
		chans[i] = ch
		go func() {
			iter := memTable.NewIterator(nil, nil)
			for i := 0; i < numEntries; i++ {
				for {
					v, err := iter.IsValid()
					if err != nil {
						ch <- err
						return
					}
					if v {
						break
					}
					// Wait for producer to catch up
					time.Sleep(100 * time.Microsecond)
				}
				curr := iter.Current()
				if fmt.Sprintf("key%010d", i) != string(curr.Key) {
					ch <- errors.New("key not expected")
				}
				if fmt.Sprintf("val%010d", i) != string(curr.Value) {
					ch <- errors.New("val not expected")
				}
				err := iter.Next()
				if err != nil {
					ch <- err
				}
			}
			v, err := iter.IsValid()
			if err != nil {
				ch <- err
				return
			}
			if v {
				ch <- errors.New("iter should not be valid")
				return
			}
			ch <- nil
		}()
	}

	for _, ch := range chans {
		err := <-ch
		require.NoError(t, err)
	}
}

func TestMTIteratorIterateInRange(t *testing.T) {
	testMTIteratorIterateInRange(t, nil, nil, 0, 99)
	testMTIteratorIterateInRange(t, []byte("prefix/key0000000033"), nil, 33, 99)
	testMTIteratorIterateInRange(t, []byte("prefix/key0000000033"), []byte("prefix/key0000000077"), 33, 76)
	testMTIteratorIterateInRange(t, nil, []byte("prefix/key0000000088"), 0, 87)
	testMTIteratorIterateInRange(t, []byte("prefix/key0000000100"), []byte("prefix/key0000000200"), 0, -1)
	testMTIteratorIterateInRange(t, []byte("prefix/key0000000100"), nil, 0, -1)
	//Important ones - test ranges that end before start of data
	testMTIteratorIterateInRange(t, []byte("prefix/j"), []byte("prefix/k"), 0, -1)
	testMTIteratorIterateInRange(t, []byte("prefix/j"), []byte("prefix/key0000000000"), 0, -1)
	// Single value
	testMTIteratorIterateInRange(t, []byte("prefix/key0000000066"), []byte("prefix/key0000000067"), 66, 66)
}

func testMTIteratorIterateInRange(t *testing.T, keyStart []byte, keyEnd []byte, expectedFirst int, expectedLast int) {
	t.Helper()
	memTable := NewMemtable(1024 * 1024)
	numEntries := 100
	batch := &Batch{
		KVs: make(map[string][]byte),
	}
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("prefix/key%010d", i)
		val := []byte(fmt.Sprintf("val%010d", i))
		batch.KVs[key] = val
	}
	ok, err := memTable.Write(batch)
	require.NoError(t, err)
	require.True(t, ok)

	iter := memTable.NewIterator(keyStart, keyEnd)
	for i := expectedFirst; i <= expectedLast; i++ {
		requireIterValid(t, iter, true)
		curr := iter.Current()
		require.Equal(t, fmt.Sprintf("prefix/key%010d", i), string(curr.Key))
		require.Equal(t, fmt.Sprintf("val%010d", i), string(curr.Value))
		err = iter.Next()
		require.NoError(t, err)
	}
	requireIterValid(t, iter, false)
}

func addToMemtable(t *testing.T, memTable *Memtable, key string, value string) {
	t.Helper()
	kvs := make(map[string][]byte)
	kvs[key] = []byte(value)
	batch := &Batch{
		KVs: kvs,
	}
	ok, err := memTable.Write(batch)
	require.NoError(t, err)
	require.True(t, ok)
}

func addToMemtableWithByteSlice(t *testing.T, memTable *Memtable, key string, value []byte) {
	t.Helper()
	kvs := make(map[string][]byte)
	kvs[key] = value
	batch := &Batch{
		KVs: kvs,
	}
	ok, err := memTable.Write(batch)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestCommonPrefix(t *testing.T) {
	testCommonPrefix(t, "someprefix", "someprefix", 10)
	testCommonPrefix(t, "somepre", "someprefix", 7)
	testCommonPrefix(t, "someprefix", "somepre", 7)
	testCommonPrefix(t, "s", "someprefix", 1)
	testCommonPrefix(t, "someprefix", "s", 1)
	testCommonPrefix(t, "otherprefix", "someprefix", 0)
	testCommonPrefix(t, "", "someprefix", 0)
	testCommonPrefix(t, "someprefix", "", 0)
}

func testCommonPrefix(t *testing.T, prefix1 string, prefix2 string, expected int) {
	t.Helper()
	cpl := findCommonPrefix([]byte(prefix1), []byte(prefix2))
	require.Equal(t, expected, cpl)
}

func requireIterValid(t *testing.T, iter iteration.Iterator, valid bool) {
	t.Helper()
	v, err := iter.IsValid()
	require.NoError(t, err)
	require.Equal(t, valid, v)
}
