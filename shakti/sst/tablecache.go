package sst

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/shakti/cloudstore"
	"sync"
)

type Cache struct {
	cache      sync.Map
	cloudStore cloudstore.Store
}

func NewTableCache(store cloudstore.Store) *Cache {
	return &Cache{cloudStore: store}
}

func (tc *Cache) GetSSTable(tableID SSTableID) (*SSTable, error) {
	skey := common.ByteSliceToStringZeroCopy(tableID)
	t, ok := tc.cache.Load(skey)
	if ok {
		return t.(*SSTable), nil //nolint:forcetypeassert
	}
	b, err := tc.cloudStore.Get(tableID)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	ssTable := &SSTable{}
	ssTable.Deserialize(b, 0)
	tc.cache.Store(skey, ssTable)
	return ssTable, nil
}

func (tc *Cache) AddSSTable(tableID SSTableID, table *SSTable) error {
	skey := common.ByteSliceToStringZeroCopy(tableID)
	tc.cache.Store(skey, table)
	return nil
}
