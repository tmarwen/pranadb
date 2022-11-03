package iteration

import (
	"github.com/squareup/pranadb/shakti/cmn"
)

// StaticIterator is used in tests only
type StaticIterator struct {
	kvs []cmn.KV
	pos int
}

func (g *StaticIterator) AddKVAsString(k string, v string) {
	g.kvs = append(g.kvs, cmn.KV{
		Key:   []byte(k),
		Value: []byte(v),
	})
}

func (g *StaticIterator) AddKV(k []byte, v []byte) {
	g.kvs = append(g.kvs, cmn.KV{
		Key:   k,
		Value: v,
	})
}

func (g *StaticIterator) Current() cmn.KV {
	if g.pos == -1 {
		return cmn.KV{}
	}
	return g.kvs[g.pos]
}

func (g *StaticIterator) Next() error {
	g.pos++
	if g.pos == len(g.kvs) {
		g.pos = -1
	}
	return nil
}

func (g *StaticIterator) IsValid() bool {
	return g.pos != -1
}
