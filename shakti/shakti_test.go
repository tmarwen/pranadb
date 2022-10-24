package shakti

import (
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestShakti(t *testing.T) {
	cloudStore := &FakeCloudStore{}
	registry := NewFakeRegistry()
	shakti := NewShakti(cloudStore, registry)
	err := shakti.Start()
	require.NoError(t, err)

	batch := NewBatch()

	batch.KVs["somekey"] = []byte("somevalue")

	err = shakti.Write(batch)
	require.NoError(t, err)

	iter, err := shakti.NewIterator(nil, nil)
	require.NoError(t, err)

	for iter.IsValid() {
		kv := iter.Current()
		log.Printf("Key:%s Value:%s", string(kv.Key), string(kv.Value))
		err = iter.Next()
		require.NoError(t, err)
	}
}
