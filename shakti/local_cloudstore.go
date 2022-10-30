package shakti

import (
	"github.com/squareup/pranadb/common"
	"sync"
)

// Simple cloud store used for testing and local development
type LocalCloudStore struct {
	store sync.Map
}

func (f *LocalCloudStore) Get(key []byte) ([]byte, error) {
	skey := common.ByteSliceToStringZeroCopy(key)
	b, ok := f.store.Load(skey)
	if !ok {
		return nil, nil
	}
	return b.([]byte), nil //nolint:forcetypeassert
}

func (f *LocalCloudStore) Add(key []byte, value []byte) error {
	skey := common.ByteSliceToStringZeroCopy(key)
	f.store.Store(skey, value)
	return nil
}

func (f *LocalCloudStore) Delete(key []byte) error {
	skey := common.ByteSliceToStringZeroCopy(key)
	f.store.Delete(skey)
	return nil
}
