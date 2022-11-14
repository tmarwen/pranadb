package clustmgr

import (
	sm "github.com/lni/dragonboat/v3/statemachine"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"io"
)

type clusterStateMachine struct {
	cm    *ClusterManager
	state clusterStateMachineState
}

func (s *clusterStateMachine) Update(bytes []byte) (sm.Result, error) {
	commandType := bytes[0]
	switch commandType {
	case nodeStartedCommandID:
		return s.handleNodeStarted(bytes)
	case nodeStoppedCommandID:
		return s.handleNodeStopped(bytes)
	case getClusterStateCommandID:
		return s.handleGetState(bytes)
	case clockTickCommandID:
		return s.handleClockTick()
	default:
		panic("unexpected command type")
	}
}

func (s *clusterStateMachine) handleNodeStarted(bytes []byte) (sm.Result, error) {
	command := &nodeStartedCommand{}
	command.deserialize(bytes[1:])
	ok, err := s.state.NodeStarted(command.nodeID, command.numNodes, command.numGroups, command.replicationFactor,
		command.windowSizeTicks, command.minTicksInWindow)
	if err != nil {
		// TODO pass back error
		log.Errorf("failed to handle register %v", err)
		return sm.Result{}, nil
	}
	v := 0
	if ok {
		v = 1
	}
	return sm.Result{
		Value: uint64(v),
	}, nil
}

func (s *clusterStateMachine) handleNodeStopped(bytes []byte) (sm.Result, error) {
	command := &nodeStoppedCommand{}
	command.deserialize(bytes)
	err := s.state.NodeStopped(command.nodeID)
	if err != nil {
		// TODO pass back error
		log.Errorf("failed to handle register %v", err)
		return sm.Result{}, nil
	}
	return sm.Result{}, nil
}

func (s *clusterStateMachine) handleGetState(bytes []byte) (sm.Result, error) {
	command := &getClusterStateCommand{}
	command.deserialize(bytes)
	state, err := s.state.GetClusterState(command.clusterName, command.nodeID, command.version)
	if err != nil {
		// TODO pass back error
		log.Errorf("failed to handle getClusterState %v", err)
		return sm.Result{}, nil
	}
	var buff []byte
	buff = state.serialize(buff)
	return sm.Result{Data: buff}, nil
}

func (s *clusterStateMachine) handleClockTick() (sm.Result, error) {
	if err := s.state.ClockTick(); err != nil {
		// TODO pass back error
		log.Errorf("failed to handle clocktick %v", err)
		return sm.Result{}, nil
	}
	return sm.Result{}, nil
}

func (s *clusterStateMachine) handleSetLeaderCommand(bytes []byte) (sm.Result, error) {
	command := &setLeaderCommand{}
	command.deserialize(bytes[1:])
	if s.state.term != 0 && s.state.term != command.term {
		// Ignore - got a message out of term
		return sm.Result{}, nil
	}
	s.state.term = command.term
	if s.state.leaderID != command.leaderID && s.state.thisNodeID == command.leaderID {
		// Becoming leader
		if err := s.cm.startTicker(s.state.clusterName); err != nil {
			return sm.Result{}, err
		}
	} else if s.state.leaderID == s.state.thisNodeID && command.leaderID != s.state.leaderID {
		// Was leader, not anymore
		if err := s.cm.stopTicker(s.state.clusterName); err != nil {
			return sm.Result{}, err
		}
	}
	return sm.Result{}, nil
}

func (s *clusterStateMachine) Lookup(i interface{}) (interface{}, error) {
	return nil, nil
}

func (s *clusterStateMachine) SaveSnapshot(writer io.Writer, collection sm.ISnapshotFileCollection, i <-chan struct{}) error {
	bytes := s.state.serialize(nil)
	buff := make([]byte, 0, len(bytes)+4)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(bytes)))
	buff = append(buff, bytes...)
	_, err := writer.Write(buff)
	return err
}

func (s *clusterStateMachine) RecoverFromSnapshot(reader io.Reader, files []sm.SnapshotFile, i <-chan struct{}) error {
	lenBuff := make([]byte, 4)
	_, err := io.ReadFull(reader, lenBuff)
	if err != nil {
		return err
	}
	l, _ := common.ReadUint32FromBufferLE(lenBuff, 0)
	bytes := make([]byte, l)
	_, err = io.ReadFull(reader, bytes)
	if err != nil {
		return err
	}
	s.state.deserialize(bytes, 0)
	return nil
}

func (s *clusterStateMachine) Close() error {
	return nil
}
