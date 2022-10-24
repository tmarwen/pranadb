package shakti

import (
	"github.com/squareup/pranadb/common"
	"sync"
)

// TODO should  LRU and L0 should always be pinned - we can have a two level cache for this
type TableCache struct {
	cache sync.Map
	cloudStore CloudStore
}

func (tc *TableCache) GetSSTable(tableID SSTableID) (*SSTable, error) {
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

func (tc *TableCache) AddSSTable(tableID SSTableID, table *SSTable) error {
	skey := common.ByteSliceToStringZeroCopy(tableID)
	tc.cache.Store(skey, table)
	return nil
}
