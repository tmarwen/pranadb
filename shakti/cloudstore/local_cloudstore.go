package cloudstore

import (
	"github.com/squareup/pranadb/common"
	"sync"
)

// LocalStore is a simple cloud store used for testing and local development
type LocalStore struct {
	store sync.Map
}

func (f *LocalStore) Get(key []byte) ([]byte, error) {
	skey := common.ByteSliceToStringZeroCopy(key)
	b, ok := f.store.Load(skey)
	if !ok {
		return nil, nil
	}
	return b.([]byte), nil //nolint:forcetypeassert
}

func (f *LocalStore) Add(key []byte, value []byte) error {
	skey := common.ByteSliceToStringZeroCopy(key)
	f.store.Store(skey, value)
	return nil
}

func (f *LocalStore) Delete(key []byte) error {
	skey := common.ByteSliceToStringZeroCopy(key)
	f.store.Delete(skey)
	return nil
}
