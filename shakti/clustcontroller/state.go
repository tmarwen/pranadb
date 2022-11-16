package clustcontroller

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"sort"
	"sync/atomic"
)

/*
How to we determine when a node can be removed from the cluster?
We want to remove nodes if they haven't called GetClusterState for some time.
We can't base the calculation on any measurement of time in the state machine as that would be non deterministic as
different replicas would likely provide different time measurements and come to different conclusions on whether a
node has expired.
We also can't let the callers pass in their own time and make the calculation based on that. In this case the
calculation would be deterministic, but different callers clocks are likely not synchronized so the conclusions on
whether to expire a node could be erroneous.
The way we overcome this is for the ClusterController to call itself (from outside Raft) periodically, this provides
a clock tick. Then we count the number of calls to GetClusterState that arrive from nodes during a window, measured
in multiples of ClockTick. At the beginning of the window we look at the currently active nodes and those nodes will
form part of the window. The window maintains a count of the amount of times GetClusterState is called per node in the window.
When the window has received a certain number of ClockTicks the window is closed and we expire any node which hasn't pinged
at least minChecksInWindow times. Then we close the window and a new one is created.
*/

type ClusterState struct {
	Version    uint64
	GroupNodes [][]int // First one is the leader
}

// Describes the state of the cluster - we keep this separate from the Raft state machine to make it easier to test
type clusterStateMachineState struct {
	clusterName       string
	thisNodeID        int
	leaderID          int
	term              uint64
	first             bool
	version           uint64
	numNodes          int
	numGroups         int
	minReplicas       int
	maxReplicas       int
	windowClockTicks  int32
	windowSizeTicks   int
	minChecksInWindow int
	activeNodes       map[int]struct{}
	pingsInWindow     map[int]int
	latestState       *ClusterState
}

func (ci *clusterStateMachineState) NodeStarted(nodeID int, numNodes int, numGroups int, minReplicas int, maxReplicas int,
	windowSizeInTicks int, minChecksInWindow int) error {
	if err := ci.checkNodeIDRange(nodeID, numNodes); err != nil {
		return err
	}
	if maxReplicas < 2 || maxReplicas > numNodes {
		return errors.Errorf("maxReplicas out of range %d", maxReplicas)
	}
	if minReplicas < 1 || minReplicas > maxReplicas {
		return errors.Errorf("minReplicas out of range %d", maxReplicas)
	}
	if windowSizeInTicks <= 0 {
		return errors.Errorf("windowSizeInTicks must be > 0")
	}
	if minChecksInWindow <= 0 {
		return errors.Errorf("minChecksInWindow must be > 0")
	}
	if numGroups <= 0 {
		return errors.Errorf("numGroups must be > 0")
	}
	if ci.first {
		// The first node to call in sets the params
		ci.version = 0 // be explicit
		ci.numNodes = numNodes
		ci.numGroups = numGroups
		ci.maxReplicas = maxReplicas
		ci.minReplicas = minReplicas
		ci.windowSizeTicks = windowSizeInTicks
		ci.minChecksInWindow = minChecksInWindow
		ci.activeNodes = make(map[int]struct{}, numNodes)
		ci.pingsInWindow = make(map[int]int, numNodes)
		ci.first = false
	} else {
		// Sanity check
		if ci.numNodes != numNodes || ci.numGroups != numGroups || ci.maxReplicas != maxReplicas ||
			ci.windowSizeTicks != windowSizeInTicks || ci.minChecksInWindow != minChecksInWindow ||
			ci.minReplicas != minReplicas {
			return errors.Error("failed to register cluster. cluster already registered with different arguments")
		}
		// When a node registers we reset its count current ping window - this prevents nodes which are quickly
		// cycling (stopping and restarting) from being considered active nodes as they would otherwise manage to get
		// enough calls to GetClusterState in a window
		if ci.pingsInWindow != nil {
			ci.pingsInWindow[nodeID] = 0
		}
	}
	return nil
}

func (ci *clusterStateMachineState) NodeStopped(nodeID int) error {
	if ci.first {
		return errors.Errorf("node %d not added", nodeID)
	}
	if err := ci.checkNodeIDRange(nodeID, ci.numNodes); err != nil {
		return err
	}
	delete(ci.pingsInWindow, nodeID)
	delete(ci.activeNodes, nodeID)
	ci.calculateClusterState()
	return nil
}

func (ci *clusterStateMachineState) ClockTick() error {
	log.Debugf("core state received clocktick for cluster %s", ci.clusterName)
	if ci.first {
		return nil
	}
	ticks := atomic.AddInt32(&ci.windowClockTicks, 1)
	if int(ticks) < ci.windowSizeTicks {
		return nil
	}
	// Close the current window
	hasChanges := false
	for nid, pc := range ci.pingsInWindow {
		if pc < ci.minChecksInWindow {
			// Any nodes which haven't pinged at least minChecksInWindow in the window will be considered expired
			delete(ci.activeNodes, nid)
			log.Debugf("state: node %d is expired as didn't receive enough pings", nid)
			hasChanges = true
		} else {
			// Any nodes that pinged enough times during a window will be considered active
			// Waiting for a whole window before considering nodes active prevents a lot of churn in recalculating
			// cluster state at startup and if node(s) are rapidly starting and stopping due to error
			_, ok := ci.activeNodes[nid]
			if !ok {
				log.Debugf("state: node %d is now joining cluster", nid)
				if ci.activeNodes == nil {
					log.Println("foo")
				}
				ci.activeNodes[nid] = struct{}{}
				hasChanges = true
			}
		}
	}
	if hasChanges {
		ci.calculateClusterState()
	}
	// Start a new window
	ci.pingsInWindow = make(map[int]int, ci.numNodes)
	for nid := range ci.activeNodes {
		ci.pingsInWindow[nid] = 0
	}
	atomic.StoreInt32(&ci.windowClockTicks, 0)
	return nil
}

func (ci *clusterStateMachineState) GetClusterState(nodeID int, version uint64) (*ClusterState, error) {
	if ci.first {
		return nil, errors.Errorf("node %d not added", nodeID)
	}
	if err := ci.checkNodeIDRange(nodeID, ci.numNodes); err != nil {
		return nil, err
	}
	// Increment the window ping count
	cnt, ok := ci.pingsInWindow[nodeID]
	if ok {
		cnt++
	} else {
		cnt = 1
	}
	ci.pingsInWindow[nodeID] = cnt
	if version != ci.version {
		if ci.latestState != nil {
			ci.latestState.Version = ci.version
		}
		return ci.latestState, nil
	}
	return nil, nil // version hasn't changed
}

func (ci *clusterStateMachineState) calculateClusterState() {
	numActiveNodes := len(ci.activeNodes)
	if numActiveNodes < ci.minReplicas {
		// Insufficient nodes we will consider the cluster inactive.
		// Note that, as we use cluster controller Raft group to manage the state, and we don't have a Raft group per
		// cluster group, then we don't need a quorum of replicas for the Prana cluster nodes to all have a consistent
		// view of the cluster state. E.g. with a Prana cluster of size 7, even if we only had two nodes active they
		// would share a consistent state - we have delegated the consensus elsewhere.
		// However, we might require a min number of replicas to satisfy our data durability requirements.
		// This is minReplicas is configured separately to replication factor and isn't just (replication_factor + 1) / 2
		ci.latestState = nil
		ci.activeNodes = make(map[int]struct{}, ci.numNodes)
		return
	}
	activeNodes := make([]int, 0, len(ci.activeNodes))
	for nid := range ci.activeNodes {
		activeNodes = append(activeNodes, nid)
	}
	// Sort to make deterministic
	sort.Ints(activeNodes)
	groupStates := make([][]int, ci.numGroups)
	var numReplicas int
	if numActiveNodes > ci.maxReplicas {
		numReplicas = ci.maxReplicas
	} else {
		numReplicas = numActiveNodes
	}
	// TODO more sophisticated algorithm - we should use consistent hashing to minimise number of group leader changes
	for i := 0; i < ci.numGroups; i++ {
		groupNodes := make([]int, 0, numReplicas)
		for r := 0; r < numReplicas; r++ {
			node := activeNodes[(i+r)%numActiveNodes]
			groupNodes = append(groupNodes, node)
		}
		groupStates[i] = groupNodes
	}
	ci.latestState = &ClusterState{
		GroupNodes: groupStates,
	}
	ci.version++
	ci.latestState.Version = ci.version
	log.Debugf("new state is %v", ci.latestState)
}

func (ci *clusterStateMachineState) serialize(buff []byte) []byte {
	buff = common.AppendStringToBufferLE(buff, ci.clusterName)
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.leaderID))
	buff = common.AppendUint64ToBufferLE(buff, ci.term)
	if ci.first {
		buff = append(buff, 1)
	} else {
		buff = append(buff, 0)
	}
	buff = common.AppendUint64ToBufferLE(buff, ci.version)
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.numNodes))
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.numGroups))
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.maxReplicas))
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.windowClockTicks))
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.windowSizeTicks))
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.minChecksInWindow))
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(ci.activeNodes)))
	for nid := range ci.activeNodes {
		buff = common.AppendUint32ToBufferLE(buff, uint32(nid))
	}
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(ci.pingsInWindow)))
	for nid, cnt := range ci.pingsInWindow {
		buff = common.AppendUint32ToBufferLE(buff, uint32(nid))
		buff = common.AppendUint32ToBufferLE(buff, uint32(cnt))
	}
	if ci.latestState != nil {
		buff = append(buff, 1)
		buff = ci.latestState.serialize(buff)
	} else {
		buff = append(buff, 0)
	}
	return buff
}

func (ci *clusterStateMachineState) deserialize(buff []byte, offset int) int {
	ci.clusterName, offset = common.ReadStringFromBufferLE(buff, offset)
	var leaderID uint32
	leaderID, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.leaderID = int(leaderID)
	ci.term, offset = common.ReadUint64FromBufferLE(buff, offset)
	ci.first = buff[offset] == 1
	offset++
	ci.version, offset = common.ReadUint64FromBufferLE(buff, offset)
	var v uint32
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.numNodes = int(v)
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.numGroups = int(v)
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.maxReplicas = int(v)
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.windowClockTicks = int32(v)
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.windowSizeTicks = int(v)
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.minChecksInWindow = int(v)
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.activeNodes = make(map[int]struct{}, v)
	for i := 0; i < int(v); i++ {
		v, offset = common.ReadUint32FromBufferLE(buff, offset)
		ci.activeNodes[int(v)] = struct{}{}
	}
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.pingsInWindow = make(map[int]int, int(v))
	for i := 0; i < int(v); i++ {
		var n, c uint32
		n, offset = common.ReadUint32FromBufferLE(buff, offset)
		c, offset = common.ReadUint32FromBufferLE(buff, offset)
		ci.pingsInWindow[int(n)] = int(c)
	}
	if buff[offset] == 1 {
		offset++
		ci.latestState = &ClusterState{}
		offset = ci.latestState.deserialize(buff, offset)
	} else {
		offset++
	}
	return offset
}

func (cs *ClusterState) serialize(buff []byte) []byte {
	buff = common.AppendUint64ToBufferLE(buff, cs.Version)
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(cs.GroupNodes)))
	for _, gs := range cs.GroupNodes {
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(gs)))
		for _, nid := range gs {
			buff = common.AppendUint32ToBufferLE(buff, uint32(nid))
		}
	}
	return buff
}

func (cs *ClusterState) deserialize(buff []byte, offset int) int {
	var v uint64
	v, offset = common.ReadUint64FromBufferLE(buff, offset)
	cs.Version = v
	var lgs uint32
	lgs, offset = common.ReadUint32FromBufferLE(buff, offset)
	groupNodes := make([][]int, int(lgs))
	for i := 0; i < int(lgs); i++ {
		var lnids uint32
		lnids, offset = common.ReadUint32FromBufferLE(buff, offset)
		groupNodes[i] = make([]int, lnids)
		for j := 0; j < int(lnids); j++ {
			var nid uint32
			nid, offset = common.ReadUint32FromBufferLE(buff, offset)
			groupNodes[i][j] = int(nid)
		}
	}
	cs.GroupNodes = groupNodes
	return offset
}

func (ci *clusterStateMachineState) checkNodeIDRange(nodeID int, numNodes int) error {
	if nodeID < 0 || nodeID >= numNodes {
		return errors.Errorf("out of range node id %d", nodeID)
	}
	return nil
}
