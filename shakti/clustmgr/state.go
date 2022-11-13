package clustmgr

import (
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
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
The way we overcome this is for the ClusterManager to call itself (from outside Raft) periodically, this provides
a clock tick. Then we count the number of calls to GetClusterState that arrive from nodes during a window, measured
in multiples of ClockTick. At the beginning of the window we look at the currently active nodes and those nodes will
form part of the window. The window maintains a count of the amount of times GetClusterState is called per node in the window.
When the window has received a certain number of ClockTicks the window is closed and we expire any node which hasn't pinged
at least minChecksInWindow times. Then we close the window and a new one is created.
*/

type ClusterState struct {
	Version     uint64
	GroupStates []GroupState
}

type GroupState struct {
	GroupID int
	NodeIDs []int // The first one is the leader
}

type clusterInfo struct {
	first bool
	version                uint64
	numNodes               int
	numGroups              int
	replicationFactor      int
	windowClockTicks       int32
	windowSizeTicks   int
	minChecksInWindow int
	activeNodes            map[int]struct{}
	pingsInWindow          map[int]int
	latestState       *ClusterState
}

func (ci *clusterInfo) NodeStarted(nodeID int, numNodes int, numGroups int, replicationFactor int,
	windowSizeInTicks int, minChecksInWindow int) (bool, error) {
	if ci.first {
		// The first node to call in sets the params
		ci.version = 0 // be explicit
		ci.numNodes = numNodes
		ci.numGroups = numGroups
		ci.replicationFactor = replicationFactor
		ci.windowSizeTicks = windowSizeInTicks
		ci.minChecksInWindow = minChecksInWindow
		ci.activeNodes = make(map[int]struct{}, numNodes)
		ci.pingsInWindow = make(map[int]int, numNodes)
		ci.first = false
	} else {
		// Sanity check
		if ci.numNodes != numNodes || ci.numGroups != numGroups || ci.replicationFactor != replicationFactor ||
			ci.windowSizeTicks != windowSizeInTicks || ci.minChecksInWindow != minChecksInWindow {
			return false, errors.Error("failed to register cluster. cluster already registered with different arguments")
		}
		// When a node registers we remove it from the current ping window - this prevents nodes which are quickly
		// cycling (stopping and restarting) from being considered active nodes as they would otherwise manage to get
		// enough calls to GetClusterState in a window
		if ci.pingsInWindow != nil {
			delete(ci.pingsInWindow, nodeID)
		}
	}
	return true, nil
}

func (ci *clusterInfo) NodeStopped(nodeID int) error {
	if ci.first {
		return nil
	}
	delete(ci.pingsInWindow, nodeID)
	delete(ci.activeNodes, nodeID)
	ci.calculateClusterState()
	return nil
}

func (ci *clusterInfo) ClockTick() error {
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
			hasChanges = true
		} else {
			// Any nodes that pinged enough times during a window will be considered active
			// Waiting for a whole window before considering nodes active prevents a lot of churn in recalculating
			// cluster state at startup and if node(s) are rapidly starting and stopping due to error
			_, ok := ci.activeNodes[nid]
			if !ok {
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
	atomic.StoreInt32(&ci.windowClockTicks, 0)
	return nil
}

func (ci *clusterInfo) GetClusterState(clusterName string, nodeID int, version uint64) (*ClusterState, error) {
	// Increment the window ping count
	cnt, ok := ci.pingsInWindow[nodeID]
	if ok {
		cnt++
	} else {
		cnt = 1
	}
	ci.pingsInWindow[nodeID] = cnt
	if version != ci.version {
		return ci.latestState, nil
	}
	return nil, nil // version hasn't changed
}

func (ci *clusterInfo) calculateClusterState() {
	numNodes := len(ci.activeNodes)
	if numNodes < ci.replicationFactor {
		// Not enough nodes in the cluster to satisfy replication factor
		ci.latestState.GroupStates = nil
		return
	}
	activeNodes := make([]int, 0, len(ci.activeNodes))
	for nid, _ := range ci.activeNodes {
		activeNodes = append(activeNodes, nid)
	}
	groupStates := make([]GroupState, ci.numGroups)
	// TODO more sophisticated algorithm - we can use consistent hashing to minimise number of group leader changes
	for i := 0; i < ci.numGroups; i++ {
		groupNodes := make([]int, 0, ci.replicationFactor)
		for r := 0; r < ci.replicationFactor; r++ {
			node := activeNodes[(i+r)%numNodes]
			groupNodes = append(groupNodes, node)
		}
		groupStates[i].GroupID = i
		groupStates[i].NodeIDs = groupNodes
	}
	ci.latestState = &ClusterState{
		Version:     ci.version + 1,
		GroupStates: groupStates,
	}
	ci.version++
}

func (ci *clusterInfo) serialize(buff []byte) []byte {
	if ci.first {
		buff = append(buff, 1)
	} else {
		buff = append(buff, 0)
	}
	buff = common.AppendUint64ToBufferLE(buff, ci.version)
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.numNodes))
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.numGroups))
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.replicationFactor))
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.windowClockTicks))
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.windowSizeTicks))
	buff = common.AppendUint32ToBufferLE(buff, uint32(ci.minChecksInWindow))
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(ci.activeNodes)))
	for nid, _ := range ci.activeNodes {
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

func (ci *clusterInfo) deserialize(buff []byte, offset int) int {
	ci.first = buff[offset] == 1
	offset++
	ci.version, offset = common.ReadUint64FromBufferLE(buff, offset)
	var v uint32
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.numNodes = int(v)
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.numGroups = int(v)
	v, offset = common.ReadUint32FromBufferLE(buff, offset)
	ci.replicationFactor = int(v)
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
	buff = common.AppendUint64ToBufferLE(buff, uint64(cs.Version))
	buff = common.AppendUint32ToBufferLE(buff, uint32(len(cs.GroupStates)))
	for _, gs := range cs.GroupStates {
		buff = common.AppendUint32ToBufferLE(buff, uint32(gs.GroupID))
		buff = common.AppendUint32ToBufferLE(buff, uint32(len(gs.NodeIDs)))
		for _, nid := range gs.NodeIDs {
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
	groupStates := make([]GroupState, int(lgs))
	for i := 0; i < int(lgs); i++ {
		var gid uint32
		gid, offset = common.ReadUint32FromBufferLE(buff, offset)
		groupStates[i].GroupID = int(gid)
		var lnids uint32
		lnids, offset = common.ReadUint32FromBufferLE(buff, offset)
		groupStates[i].NodeIDs = make([]int, lnids)
		for j := 0; j < int(lnids); i++ {
			var nid uint32
			nid, offset = common.ReadUint32FromBufferLE(buff, offset)
			groupStates[i].NodeIDs[j] = int(nid)
		}
	}
	return offset
}
