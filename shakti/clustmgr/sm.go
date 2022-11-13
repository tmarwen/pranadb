package clustmgr

import (
	sm "github.com/lni/dragonboat/v3/statemachine"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"io"
)

type clusterStateMachine struct {
	cm *ClusterManager
	ci clusterInfo
	clusterName string
	thisNodeID int
	leaderID int
	term uint64
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

func (s *clusterStateMachine) handleNodeStarted(bytes []byte) (sm.Result, error){
	command := &nodeStartedCommand{}
	command.deserialize(bytes[1:])
	ok, err := s.ci.NodeStarted(command.nodeID, command.numNodes, command.numGroups, command.replicationFactor,
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

func (s *clusterStateMachine) handleNodeStopped(bytes []byte) (sm.Result, error){
	command := &nodeStoppedCommand{}
	command.deserialize(bytes)
	err := s.ci.NodeStopped(command.nodeID)
	if err != nil {
		// TODO pass back error
		log.Errorf("failed to handle register %v", err)
		return sm.Result{}, nil
	}
	return sm.Result{}, nil
}

func (s *clusterStateMachine) handleGetState(bytes []byte) (sm.Result, error){
	command := &getClusterStateCommand{}
	command.deserialize(bytes)
	state, err := s.ci.GetClusterState(command.clusterName, command.nodeID, command.version)
	if err != nil {
		// TODO pass back error
		log.Errorf("failed to handle getClusterState %v", err)
		return sm.Result{}, nil
	}
	var buff []byte
	buff = state.serialize(buff)
	return sm.Result{Data: buff}, nil
}

func (s *clusterStateMachine) handleClockTick() (sm.Result, error){
	if err := s.ci.ClockTick(); err != nil {
		// TODO pass back error
		log.Errorf("failed to handle clocktick %v", err)
		return sm.Result{}, nil
	}
	return sm.Result{}, nil
}

func (s *clusterStateMachine) handleSetLeaderCommand(bytes []byte) (sm.Result, error) {
	command := &setLeaderCommand{}
	command.deserialize(bytes[1:])
	if s.term != 0 && s.term != command.term {
		// Ignore - got a message out of term
		return sm.Result{}, nil
	}
	s.term = command.term
	if s.leaderID != command.leaderID && s.thisNodeID == command.leaderID {
		// Becoming leader
		if err := s.cm.startTicker(s.clusterName); err != nil {
			return sm.Result{}, err
		}
	} else if s.leaderID == s.thisNodeID && command.leaderID != s.leaderID {
		// Was leader, not anymore
		if err := s.cm.stopTicker(s.clusterName); err != nil {
			return sm.Result{}, nil
		}
	}
	return sm.Result{}, nil
}

func (s *clusterStateMachine) Lookup(i interface{}) (interface{}, error) {
	return nil, nil
}

func (s *clusterStateMachine) SaveSnapshot(writer io.Writer, collection sm.ISnapshotFileCollection, i <-chan struct{}) error {
	var bytes []byte
	bytes = common.AppendStringToBufferLE(bytes, s.clusterName)
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(s.leaderID))
	bytes = common.AppendUint64ToBufferLE(bytes, s.term)
	bytes = s.ci.serialize(bytes)
	buff := make([]byte, 0, len(bytes) + 4)
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
	offset := 0
	s.clusterName, offset = common.ReadStringFromBufferLE(bytes, offset)
	var leaderID uint32
	leaderID, offset = common.ReadUint32FromBufferLE(bytes, offset)
	s.leaderID = int(leaderID)
	s.term, offset = common.ReadUint64FromBufferLE(bytes, offset)
	s.ci.deserialize(bytes, offset)
	return nil
}

func (s *clusterStateMachine) Close() error {
	return nil
}