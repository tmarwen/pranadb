package cmn

type NoopReplicator struct {
}

func (n NoopReplicator) ReplicateMessage(message []byte) error {
	return nil
}
