package shakti

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFindOffset(t *testing.T) {
	commonPrefix := []byte("foo/")

	gi := &genIter{}
	for i := 0; i < 10; i++ {
		gi.addKVAsString(fmt.Sprintf("foo/somekey%02d", i), fmt.Sprintf("somevalue%02d", i))
	}

	sstable, _, _, err := BuildSSTable(commonPrefix, gi)
	require.NoError(t, err)

	iter, err := sstable.NewIterator([]byte("foo/somekey04"), []byte("foo/somekey07"))
	require.NoError(t, err)

	for iter.IsValid() {
		kv := iter.Current()
		log.Printf("key:%s value %s", string(kv.Key), string(kv.Value))
		err = iter.Next()
		require.NoError(t, err)
	}
}

type genIter struct {
	kvs []KV
	pos int
}

func (g *genIter) addKVAsString(k string, v string) {
	g.kvs = append(g.kvs, KV{
		Key:   []byte(k),
		Value: []byte(v),
	})
}

func (g *genIter) Current() KV {
	if g.pos == -1 {
		return KV{}
	}
	return g.kvs[g.pos]
}

func (g *genIter) Next() error {
	g.pos++
	if g.pos == len(g.kvs) {
		g.pos = -1
	}
	return nil
}

func (g *genIter) IsValid() bool {
	return g.pos != -1
}

