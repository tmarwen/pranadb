package clustcontroller

import (
	log "github.com/sirupsen/logrus"
	pranalog "github.com/squareup/pranadb/log"
	"github.com/stretchr/testify/require"
	"testing"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:            true,
		DisableQuote:           true,
		FullTimestamp:          true,
		DisableLevelTruncation: true,
		TimestampFormat:        pranalog.TimestampFormat,
	})
	log.SetLevel(log.TraceLevel)
}

/*
TODO!!!
1. Test with minTicksInWindow > 1
2. Test NodeStopped
*/

func TestBoundsChecks(t *testing.T) {
	state := newClusterStateMachineState()
	err := state.NodeStarted(-1, 3, 10, 3, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(3, 3, 10, 3, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(0, -1, 10, 3, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(0, 3, -1, 3, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(0, 3, 0, 3, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(0, 3, 10, 0, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(0, 3, 10, -1, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(0, 3, 10, 4, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(0, 3, 10, 2, 3, 0, 1)
	require.Error(t, err)
	err = state.NodeStarted(0, 3, 10, 2, 3, -1, 1)
	require.Error(t, err)
	err = state.NodeStarted(0, 3, 10, 2, 3, 5, 0)
	require.Error(t, err)
	err = state.NodeStarted(0, 3, 10, 2, 3, 5, -1)
	require.Error(t, err)
}

func TestGetClusterStateNotStarted(t *testing.T) {
	state := newClusterStateMachineState()
	cs, err := state.GetClusterState(1, 0)
	require.Nil(t, cs)
	require.Error(t, err)
}

func TestGetClusterStateInvalidNodeID(t *testing.T) {
	state := newClusterStateMachineState()
	err := state.NodeStarted(1, 3, 4, 3, 3, 1, 5)
	require.NoError(t, err)
	cs, err := state.GetClusterState(-1, 0)
	require.Error(t, err)
	require.Nil(t, cs)
	cs, err = state.GetClusterState(3, 0)
	require.Error(t, err)
	require.Nil(t, cs)
	cs, err = state.GetClusterState(4, 0)
	require.Error(t, err)
	require.Nil(t, cs)
}

func TestNodeStartMoreThanOnceDifferentArgs(t *testing.T) {
	state := newClusterStateMachineState()
	err := state.NodeStarted(1, 5, 10, 3, 3, 5, 1)
	require.NoError(t, err)
	err = state.NodeStarted(1, 7, 10, 3, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(1, 5, 20, 3, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(1, 5, 10, 3, 5, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(1, 5, 10, 1, 3, 5, 1)
	require.Error(t, err)
	err = state.NodeStarted(1, 5, 10, 3, 3, 10, 1)
	require.Error(t, err)
	err = state.NodeStarted(1, 5, 10, 3, 3, 5, 3)
	require.Error(t, err)
	err = state.NodeStarted(1, 5, 10, 3, 3, 5, 1)
	require.NoError(t, err)
}

func TestNoStateGotVersionAlready(t *testing.T) {
	state := newClusterStateMachineState()

	windowSizeTicks := 5
	minChecksInWindow := 1

	err := state.NodeStarted(0, 3, 4, 3, 3, windowSizeTicks, minChecksInWindow)
	require.NoError(t, err)
	err = state.NodeStarted(1, 3, 4, 3, 3, windowSizeTicks, minChecksInWindow)
	require.NoError(t, err)
	err = state.NodeStarted(2, 3, 4, 3, 3, windowSizeTicks, minChecksInWindow)
	require.NoError(t, err)

	// All nodes ping once
	for i := 0; i < 3; i++ {
		cs, err := state.GetClusterState(i, 0)
		require.NoError(t, err)
		require.Nil(t, cs)
	}
	for i := 0; i < windowSizeTicks; i++ {
		err = state.ClockTick()
		require.NoError(t, err)
	}
	// Now there should be state
	for i := 0; i < 3; i++ {
		cs, err := state.GetClusterState(i, 0)
		require.NoError(t, err)
		require.NotNil(t, cs)
		require.Equal(t, 1, int(cs.Version))
	}
	// Now try and get state again specifying version as 1 - current version is 1 so we shouldn't be sent any
	for i := 0; i < 3; i++ {
		cs, err := state.GetClusterState(i, 1)
		require.NoError(t, err)
		require.Nil(t, cs)
	}
}

func TestNoPingNoState(t *testing.T) {
	f := func(t *testing.T, state *clusterStateMachineState) {} //nolint:thelper
	testStateChange(t, 3, 3, 3, nil, 0, nil, 0, f)
}

func TestAddSingleNodeInWindow(t *testing.T) {
	f := func(t *testing.T, state *clusterStateMachineState) { //nolint:thelper
		_, err := state.GetClusterState(0, 0)
		require.NoError(t, err)
	}
	testStateChange(t, 3, 3, 3, nil, 0, nil, 0, f)
}

func TestAddAllNodesInWindow(t *testing.T) {
	f := func(t *testing.T, state *clusterStateMachineState) { //nolint:thelper
		for i := 0; i < 3; i++ {
			cs, err := state.GetClusterState(i, 0)
			require.NoError(t, err)
			// The state will be nil until the window is closed
			require.Nil(t, cs)
		}
	}
	testStateChange(t, 3, 3, 3, nil, 0, []int{0, 1, 2}, 1, f)
}

func TestAddMinReplicasNodesInWindow(t *testing.T) {
	minReplicas := 3
	f := func(t *testing.T, state *clusterStateMachineState) { //nolint:thelper
		for i := 0; i < minReplicas; i++ {
			_, err := state.GetClusterState(i, 0)
			require.NoError(t, err)
		}
	}
	testStateChange(t, 5, minReplicas, 5, nil, 0, []int{0, 1, 2}, 1, f)
}

func TestAddBetweenMinAndMaxReplicasInWindow(t *testing.T) {
	// Add 4 nodes
	f := func(t *testing.T, state *clusterStateMachineState) { //nolint:thelper
		for i := 0; i < 4; i++ {
			_, err := state.GetClusterState(i, 0)
			require.NoError(t, err)
		}
	}
	testStateChange(t, 5, 3, 5, nil, 0, []int{0, 1, 2, 3}, 1, f)
}

func TestAddLessThanMinReplicasNodesInWindow(t *testing.T) {
	minReplicas := 3
	f := func(t *testing.T, state *clusterStateMachineState) { //nolint:thelper
		for i := 0; i < minReplicas-1; i++ {
			_, err := state.GetClusterState(i, 0)
			require.NoError(t, err)
		}
	}
	// Should be no state
	testStateChange(t, 5, minReplicas, 5, nil, 0, nil, 1, f)
}

func TestExpireNodesSoStillHaveMinReplicasNodes(t *testing.T) {
	f := func(t *testing.T, state *clusterStateMachineState) { //nolint:thelper
		// Ping 0 and 2 only so 1 should expire
		_, err := state.GetClusterState(0, 0)
		require.NoError(t, err)
		_, err = state.GetClusterState(2, 0)
		require.NoError(t, err)
	}
	testStateChange(t, 3, 2, 3, []int{0, 1, 2}, 0, []int{0, 2}, 1, f)
}

func TestExpireSoThereAreBetweenMinAndMaxReplicasNodes(t *testing.T) {
	f := func(t *testing.T, state *clusterStateMachineState) { //nolint:thelper
		_, err := state.GetClusterState(0, 0)
		require.NoError(t, err)
		_, err = state.GetClusterState(1, 0)
		require.NoError(t, err)
		_, err = state.GetClusterState(3, 0)
		require.NoError(t, err)
		_, err = state.GetClusterState(4, 0)
		require.NoError(t, err)
	}
	testStateChange(t, 5, 3, 5, []int{0, 1, 2, 3, 4}, 0, []int{0, 1, 3, 4}, 1, f)
}

func TestReaddingNodeShouldRemoveItFromWindow(t *testing.T) {
	f := func(t *testing.T, state *clusterStateMachineState) { //nolint:thelper
		// Ping all the nodes
		_, err := state.GetClusterState(0, 0)
		require.NoError(t, err)
		_, err = state.GetClusterState(1, 0)
		require.NoError(t, err)
		_, err = state.GetClusterState(2, 0)
		require.NoError(t, err)
		// Then re-add node 1 - should get removed
		err = state.NodeStarted(1, 3, 10, 2, 3, 5, 1)
		require.NoError(t, err)
	}
	testStateChange(t, 3, 2, 3, []int{0, 1, 2}, 0, []int{0, 2}, 1, f)
}

func TestExpireAllNodes(t *testing.T) {
	f := func(t *testing.T, state *clusterStateMachineState) { //nolint:thelper
		// Don't ping at all!
	}
	// Should be all expired so no state
	testStateChange(t, 3, 3, 3, []int{0, 1, 2}, 1, nil, 0, f)
}

func testStateChange(t *testing.T, clusterSize int, minReplicas int, maxReplicas int, initialActiveNodes []int, //nolint:thelper
	initialVersion int, expectedActiveNodes []int, expectedFinalVersion int, action func(t *testing.T, state *clusterStateMachineState)) {
	windowSizeTicks := 5
	numGroups := 10
	state := newClusterStateMachineState()
	for i := 0; i < clusterSize; i++ {
		err := state.NodeStarted(i, clusterSize, numGroups, minReplicas, maxReplicas, windowSizeTicks, 1)
		require.NoError(t, err)
	}
	state.activeNodes = toNodeSet(initialActiveNodes)
	state.version = uint64(initialVersion)
	for nid := range state.activeNodes {
		state.pingsInWindow[nid] = 0
	}
	// Now run the action
	action(t, state)
	// Close the window
	for i := 0; i < windowSizeTicks; i++ {
		err := state.ClockTick()
		require.NoError(t, err)
	}
	// Now validate the end state
	expectedSet := toNodeSet(expectedActiveNodes)
	require.Equal(t, len(expectedSet), len(state.activeNodes))
	for n := range expectedSet {
		_, ok := state.activeNodes[n]
		require.True(t, ok)
	}
	for i := 0; i < clusterSize; i++ {
		cs, err := state.GetClusterState(i, 0)
		require.NoError(t, err)
		lan := len(expectedActiveNodes)
		if lan < minReplicas {
			require.Nil(t, cs)
		} else {
			require.NotNil(t, cs)
			require.Equal(t, expectedFinalVersion, int(cs.Version))
			var numReplicas int
			if lan < maxReplicas {
				numReplicas = lan
			} else {
				numReplicas = maxReplicas
			}
			verifyClusterState(t, cs, numGroups, numReplicas, expectedActiveNodes)
		}
	}
}

func verifyClusterState(t *testing.T, cs *ClusterState, numGroups int, numReplicas int, //nolint:thelper
	expectedActiveNodes []int) {
	require.Equal(t, numGroups, len(cs.GroupNodes))
	activeNodesSet := toNodeSet(expectedActiveNodes)
	for _, nodeIDs := range cs.GroupNodes {
		require.Equal(t, numReplicas, len(nodeIDs))
		nodeSet := toNodeSet(nodeIDs)
		require.Equal(t, numReplicas, len(nodeSet)) // They must be unique
		for _, nid := range nodeIDs {
			_, ok := activeNodesSet[nid]
			// Must be in the active node set
			if !ok {
				log.Println("foo")
			}
			require.True(t, ok)
		}
	}
}

func toNodeSet(nodes []int) map[int]struct{} {
	set := make(map[int]struct{})
	for _, n := range nodes {
		set[n] = struct{}{}
	}
	return set
}

func newClusterStateMachineState() *clusterStateMachineState {
	state := &clusterStateMachineState{}
	state.first = true
	return state
}

func TestSerializeDeserializeClusterTest(t *testing.T) {
	cs := &ClusterState{
		Version:    23,
		GroupNodes: [][]int{{0, 1, 2}, {2, 1, 0}, {2, 0, 1}, {1, 0, 2}},
	}
	buff := cs.serialize(nil)
	require.NotNil(t, buff)
	cs2 := &ClusterState{}
	cs2.deserialize(buff, 0)
	require.Equal(t, cs, cs2)
}
