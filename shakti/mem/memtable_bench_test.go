package mem

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func BenchmarkMemTableWrites(b *testing.B) {
	numEntries := 1000
	batch := &Batch{
		KVs: make(map[string][]byte),
	}
	for i := 0; i < numEntries; i++ {
		k := rand.Intn(100000)
		key := fmt.Sprintf("prefix/key%010d", k)
		val := []byte(fmt.Sprintf("val%010d", k))
		batch.KVs[key] = val
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		memTable := NewMemtable(1024 * 1024)
		ok, err := memTable.Write(batch)
		require.NoError(b, err)
		require.True(b, ok)
	}
}
