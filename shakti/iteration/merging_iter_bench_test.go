package iteration

import (
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func BenchmarkMergingIterator(b *testing.B) {
	numEntries := 1000
	numIters := 10
	iters := make([]Iterator, numIters)
	for i := 0; i < numIters; i++ {
		iters[i] = &StaticIterator{}
	}
	var expectedKeys [][]byte
	var expectedVals [][]byte
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("someprefix/key-%010d", i))
		val := []byte(fmt.Sprintf("someprefix/val-%010d", i))
		expectedKeys = append(expectedKeys, key)
		expectedVals = append(expectedVals, val)
		r := rand.Intn(numIters)
		sIter := iters[r].(*StaticIterator) //nolint:forcetypeassert
		sIter.AddKV(key, val)
	}

	log.Printf("n is %d", b.N)

	for i := 0; i < b.N; i++ {

		//log.Printf("Iteration is %d", i)

		//b.StopTimer()

		for j := 0; j < numIters; j++ {
			iters[j].(*StaticIterator).pos = 0
		}

		mi, err := NewMergingIterator(iters, false)
		require.NoError(b, err)

		//b.StartTimer()

		for j := 0; j < numEntries; j++ {
			valid, err := mi.IsValid()
			if err != nil {
				panic(err)
			}
			if !valid {
				panic("not valid")
			}
			curr := mi.Current()
			//b.StopTimer()
			expectedKey := expectedKeys[j]
			expectedVal := expectedVals[j]
			//log.Printf("got key %s", curr.Key)
			if !bytes.Equal(expectedKey, curr.Key) {
				panic("key not equal")
			}
			if !bytes.Equal(expectedVal, curr.Value) {
				panic("key not equal")
			}
			//b.StartTimer()
			err = mi.Next()
			if err != nil {
				panic(err)
			}
		}
	}
}
