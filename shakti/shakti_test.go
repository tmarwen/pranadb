package shakti

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFoo(t *testing.T) {
	conf := Conf{
		WriteFormat:               FormatV1,
		MemtableMaxSizeBytes:      10 * 1024 * 1024,
		MemtableFlushQueueMaxSize: 2,
	}
	cloudStore := &LocalCloudStore{}
	registry := NewRegistry(cloudStore, conf)
	shakti := NewShakti(cloudStore, registry, conf)
	err := shakti.Start()
	require.NoError(t, err)

	/*
	TODO
	Write a bunch of entries until a bunch of SSTables appear in cloud store (and time it).
	Then iterate through them, and verify all there.

	Do this with
	a) Topic like data - incrementing offset
	b) Same set of keys updating a lot
	c) Updating set of keys, and deleting some of them
	 */

}
