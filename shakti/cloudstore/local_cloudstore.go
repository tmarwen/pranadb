package cloudstore

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"sync"
	"time"
)

func NewLocalStore(delay time.Duration) *LocalStore {
	return &LocalStore{delay: delay}
}

// LocalStore is a simple cloud store used for testing and local development
type LocalStore struct {
	store sync.Map
	delay time.Duration
}

func (f *LocalStore) Get(key []byte) ([]byte, error) {
	f.maybeAddDelay()
	skey := common.ByteSliceToStringZeroCopy(key)
	b, ok := f.store.Load(skey)
	if !ok {
		return nil, nil
	}
	return b.([]byte), nil //nolint:forcetypeassert
}

func (f *LocalStore) Add(key []byte, value []byte) error {
	f.maybeAddDelay()
	skey := common.ByteSliceToStringZeroCopy(key)
	log.Debugf("local cloud store adding blob with key %s value length %d", skey, len(value))
	f.store.Store(skey, value)
	return nil
}

func (f *LocalStore) Delete(key []byte) error {
	f.maybeAddDelay()
	skey := common.ByteSliceToStringZeroCopy(key)
	f.store.Delete(skey)
	return nil
}

func (f *LocalStore) maybeAddDelay() {
	if f.delay != 0 {
		time.Sleep(f.delay)
	}
}

func (f *LocalStore) Size() int {
	size := 0
	f.store.Range(func(_, _ interface{}) bool {
		size++
		return true
	})
	return size
}
