package shakti

import (
	"github.com/squareup/pranadb/common"
	"sync"
)

type FakeCloudStore struct {
	store sync.Map
}

func (f *FakeCloudStore) Get(key []byte) ([]byte, error) {
	skey := common.ByteSliceToStringZeroCopy(key)
	b, ok := f.store.Load(skey)
	if !ok {
		return nil, nil
	}
	return b.([]byte), nil //nolint:forcetypeassert
}

func (f *FakeCloudStore) Add(key []byte, value []byte) error {
	skey := common.ByteSliceToStringZeroCopy(key)
	f.store.Store(skey, value)
	return nil
}

func (f *FakeCloudStore) Delete(key []byte) error {
	skey := common.ByteSliceToStringZeroCopy(key)
	f.store.Delete(skey)
	return nil
}

