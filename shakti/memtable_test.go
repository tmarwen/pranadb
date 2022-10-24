package shakti

import (
	"github.com/andy-kimball/arenaskl"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSkiplist(t *testing.T) {
	sl := arenaskl.NewSkiplist(arenaskl.NewArena(1024 * 1024))

	var it arenaskl.Iterator
	it.Init(sl)

	err := it.Add([]byte("key1"), []byte("val1"), 0)
	require.NoError(t, err)

	err = it.Add([]byte("key0"), []byte("val0"), 0)
	require.NoError(t, err)

	err = it.Add([]byte("key3"), []byte("val3"), 0)
	require.NoError(t, err)

	err = it.Add([]byte("key2"), []byte("val2"), 0)
	require.NoError(t, err)

	log.Printf("key %s val %s", string(it.Key()), string(it.Value()))
	it.Next()
	log.Printf("key %s val %s", string(it.Key()), string(it.Value()))
	it.Seek([]byte("key1"))
	log.Printf("key %s val %s", string(it.Key()), string(it.Value()))
	it.SeekToFirst()
	log.Printf("key %s val %s", string(it.Key()), string(it.Value()))
	it.SeekToLast()
	log.Printf("key %s val %s", string(it.Key()), string(it.Value()))
	it.Next()
	log.Printf("valid %t", it.Valid())
	it.Seek([]byte("key2"))

	err = it.Add([]byte("key9"), []byte("val9"), 0)
	require.NoError(t, err)
	log.Printf("key %s val %s", string(it.Key()), string(it.Value()))
	it.Prev()
	log.Printf("key %s val %s", string(it.Key()), string(it.Value()))
}
