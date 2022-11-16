package clustcontroller

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	sm "github.com/lni/dragonboat/v3/statemachine"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"io"
)

type clusterStateMachine struct {
	cm    *ClusterController
	state clusterStateMachineState
}

func newClusterStateMachine(cm *ClusterController) *clusterStateMachine {
	csm := &clusterStateMachine{cm: cm}
	csm.state.first = true
	return csm
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
	case setLeaderCommandID:
		return s.handleSetLeaderCommand(bytes)
	default:
		panic(fmt.Sprintf("unexpected command type %d", commandType))
	}
}

func (s *clusterStateMachine) handleNodeStarted(bytes []byte) (sm.Result, error) {
	b := proto.NewBuffer(bytes[1:])
	message := &clustermsgs.NodeStartedMessage{}
	if err := b.Unmarshal(message); err != nil {
		return sm.Result{}, err
	}
	err := s.state.NodeStarted(int(message.NodeId), int(message.NumNodes), int(message.NumGroups), int(message.MinReplicas),
		int(message.MaxReplicas), int(message.WindowSizeTicks), int(message.MinTicksInWindow))
	if err != nil {
		return errorResult(err), nil
	}
	return nilResult, nil
}

func (s *clusterStateMachine) handleNodeStopped(bytes []byte) (sm.Result, error) {
	b := proto.NewBuffer(bytes[1:])
	message := &clustermsgs.NodeStoppedMessage{}
	if err := b.Unmarshal(message); err != nil {
		return sm.Result{}, err
	}
	err := s.state.NodeStopped(int(message.NodeId))
	if err != nil {
		return errorResult(err), nil
	}
	return nilResult, nil
}

func (s *clusterStateMachine) handleGetState(bytes []byte) (sm.Result, error) {
	b := proto.NewBuffer(bytes[1:])
	message := &clustermsgs.GetClusterStateMessage{}
	if err := b.Unmarshal(message); err != nil {
		return sm.Result{}, err
	}
	state, err := s.state.GetClusterState(int(message.NodeId), uint64(message.Version))
	if err != nil {
		return errorResult(err), nil
	}
	buff := []byte{1}
	if state != nil {
		buff = state.serialize(buff)
	}
	return sm.Result{Data: buff}, nil
}

func (s *clusterStateMachine) handleClockTick() (sm.Result, error) {
	if err := s.state.ClockTick(); err != nil {
		return errorResult(err), nil
	}
	return nilResult, nil
}

func (s *clusterStateMachine) handleSetLeaderCommand(bytes []byte) (sm.Result, error) {
	command := &setLeaderCommand{}
	command.deserialize(bytes[1:])
	if s.state.term != 0 && command.term <= s.state.term {
		// Ignore - got a message out of term
		log.Debug("ignoring out of term")
		return nilResult, nil
	}
	var err error
	s.state.term = command.term
	log.Debugf("In handleSetLeaderCommand in node %d new leader is %d term is %d", s.state.thisNodeID, command.leaderID, command.term)
	if s.state.leaderID != command.leaderID && s.state.thisNodeID == command.leaderID {
		// Becoming leader
		err = s.cm.becomeLeader(s.state.clusterName, true)
	} else if s.state.leaderID == s.state.thisNodeID && command.leaderID != s.state.leaderID {
		// Was leader, not anymore
		err = s.cm.becomeLeader(s.state.clusterName, false)
	}
	if err != nil {
		return errorResult(err), nil
	}
	return nilResult, nil
}

func errorResult(err error) sm.Result {
	buff := []byte{0}
	buff = common.AppendStringToBufferLE(buff, err.Error())
	return sm.Result{Data: buff}
}

var nilResult = sm.Result{Data: []byte{1}}

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
