package cmn

type Replicator interface {
	ReplicateMessage(message []byte) error
}

type Replica interface {
	ReceiveReplicationMessage(message []byte) error
}

type NoopReplicator struct {
}

func (n NoopReplicator) ReplicateMessage(message []byte) error {
	return nil
}
