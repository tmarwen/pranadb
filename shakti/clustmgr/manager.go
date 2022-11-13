package clustmgr

import (
	"context"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	sm "github.com/lni/dragonboat/v3/statemachine"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"path/filepath"
	"time"

	"sync"
)

type ClusterManager struct {
	conf Config
	nodeHost *dragonboat.NodeHost
	clients sync.Map
	clientsByClusterID sync.Map
}

const (
	// cluster state machine command
	nodeStartedCommandID = 1
	nodeStoppedCommandID = 2
	getClusterStateCommandID = 3
	clockTickCommandID       = 4
	setLeaderCommandID = 5
)

type ClusterConfig struct {
	ClusterName string
	ClusterID uint64
}

type Config struct {
	ClusterManagerClusterID uint64
	NodeID int
	DataDir string
	RaftListenAddresses []string
	Clusters []ClusterConfig
	RaftRTTMs uint64
	RaftElectionRTT uint64
	RaftHeartbeatRTT uint64
	RaftMetricsEnabled bool
	SequenceSnapshotEntries uint64
	CompactionOverhead uint64
	IntraClusterTLSConfig conf.TLSConfig
	TickTimerPeriod time.Duration
}

func NewClusterManager(conf Config) *ClusterManager {
	return &ClusterManager{
		conf:     conf,
	}
}

func (cm *ClusterManager) Start() error {

	for _, cc := range cm.conf.Clusters {
		ccm, err := cm.newClientClusterManager(cc.ClusterID)
		if err != nil {
			return err
		}
		cm.clients.Store(cc.ClusterName, ccm)
		cm.clientsByClusterID.Store(cc.ClusterID, ccm)
	}

	// Start the node host
	nodeAddress := cm.conf.RaftListenAddresses[cm.conf.NodeID]
	dragonBoatDir := filepath.Join(cm.conf.DataDir, "raft")
	nhc := config.NodeHostConfig{
		DeploymentID:   cm.conf.ClusterManagerClusterID,
		WALDir:         dragonBoatDir,
		NodeHostDir:    dragonBoatDir,
		RTTMillisecond: uint64(cm.conf.RaftRTTMs),
		RaftAddress:    nodeAddress,
		EnableMetrics:  cm.conf.RaftMetricsEnabled,
		Expert:         config.GetDefaultExpertConfig(),
		RaftEventListener: cm,
	}
	if cm.conf.IntraClusterTLSConfig.Enabled {
		nhc.MutualTLS = true
		nhc.CAFile = cm.conf.IntraClusterTLSConfig.ClientCertsPath
		nhc.CertFile = cm.conf.IntraClusterTLSConfig.CertPath
		nhc.KeyFile = cm.conf.IntraClusterTLSConfig.KeyPath
	}
	// There appears to a bug in Dragonboat where if LogDB is busy, the proposals queue gets paused but never gets
	// unpaused when LogDB is not busy any more. So we added a new config setting to enable the pause. This is ok
	// for our usage as we limit the number of concurrent proposals anyway at our end - each shard shard processor
	// will only have one outstanding proposal at any one time
	nhc.Expert.IgnorePauseOnProposals = true
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return err
	}
	cm.nodeHost = nh
	// Every node is a member of the raft group
	initialMembers := make(map[uint64]string)
	for nid, listenAddress := range cm.conf.RaftListenAddresses {
		initialMembers[uint64(nid) + 1] = listenAddress
	}
	// We start the cluster raft groups in parallel
	var chans []chan error
	for _, cc := range cm.conf.Clusters {
		// Start the state machines
		rc := createRaftGroupConfig(cm.conf.NodeID, cc.ClusterID, cm.conf)
		ch := make(chan error, 1)
		chans = append(chans, ch)
		go func() {
			factory := func (_ uint64, _ uint64) sm.IStateMachine {
				return &clusterStateMachine{
					cm: cm,
					thisNodeID: cm.conf.NodeID,
					leaderID: -1,
					clusterName: cc.ClusterName,
				}
			}
			ch <- nh.StartCluster(initialMembers, false, factory, rc)
		}()
	}
	for _, ch := range chans {
		err := <- ch
		if err != nil {
			return err
		}
	}
	return nil
}

func createRaftGroupConfig(nodeID int, clusterID uint64, conf Config) config.Config {
	return config.Config{
		NodeID:             uint64(nodeID + 1),
		ElectionRTT:        conf.RaftElectionRTT,
		HeartbeatRTT:       conf.RaftHeartbeatRTT,
		CheckQuorum:        true,
		SnapshotEntries:    conf.SequenceSnapshotEntries,
		CompactionOverhead: conf.CompactionOverhead,
		ClusterID:          clusterID, // Reserved for the ticker SM
	}
}

func (cm *ClusterManager) startTicker(clusterName string) error {
	c, err := cm.getClient(clusterName)
	if err != nil {
		return err
	}
	c.startTicker()
	return nil
}

func (cm *ClusterManager) stopTicker(clusterName string) error {
	c, err := cm.getClient(clusterName)
	if err != nil {
		return err
	}
	c.stopTicker()
	return nil
}

func (cm *ClusterManager) Stop() error {
	cm.nodeHost.Stop()
	return nil
}

func (cm *ClusterManager) NodeStarted(clusterName string, clusterID uint64, nodeID int, numNodes int, numGroups int,
	replicationFactor int, windowSizeInPings int, minPingsInWindow int) (bool, error) {
	c, err := cm.getClient(clusterName)
	if err != nil {
		return false, err
	}
	return c.nodeStarted(clusterName, clusterID, nodeID, numNodes, numGroups, replicationFactor, windowSizeInPings, minPingsInWindow)
}

func (cm *ClusterManager) NodeStopped(clusterName string, nodeID int) error {
	c, err := cm.getClient(clusterName)
	if err != nil {
		return err
	}
	return c.nodeStopped(nodeID)
}

func (cm *ClusterManager) GetClusterState(clusterName string, nodeID int, version uint64) (*ClusterState, error) {
	c, err := cm.getClient(clusterName)
	if err != nil {
		return nil, err
	}
	return c.getClusterState(clusterName, nodeID, version)
}

func (cm *ClusterManager) ClockTick(clusterName string) error {
	c, err := cm.getClient(clusterName)
	if err != nil {
		return err
	}
	return c.clockTick()
}

func (cm *ClusterManager) getClient(clusterName string) (*clientClusterManager, error) {
	o, ok := cm.clients.Load(clusterName)
	if !ok {
		// All clusters must be configured in the config file
		return nil, errors.Errorf("unknown cluster %cm has it been configured in the cluster manager config?", clusterName)
	}
	return o.(*clientClusterManager), nil
}

func (cm *ClusterManager) LeaderUpdated(info raftio.LeaderInfo) {
	if info.NodeID != uint64(cm.conf.NodeID) + 1 {
		panic("received leader info on wrong node")
	}
	newLeaderID := int(info.LeaderID - 1)
	if newLeaderID == cm.conf.NodeID {
		// If the current node has become leader of a group we call setLeader
		clusterID := info.ClusterID - 1
		o, ok := cm.clientsByClusterID.Load(clusterID)
		if !ok {
			// All clusters must be configured in the config file
			log.Errorf("unknown cluster id %d", clusterID)
		}
		ccm := o.(*clientClusterManager)
		if err := ccm.setLeader(newLeaderID, info.Term); err != nil {
			log.Errorf("failed to set leader %v", err)
		}
	}
}

func (cm *ClusterManager) newClientClusterManager(clusterID uint64) (*clientClusterManager, error) {
	sess, err := cm.nodeHost.SyncGetSession(context.Background(), clusterID)
	if err != nil {
		return nil, err
	}
	return &clientClusterManager{
		cm:        cm,
		cs:        sess,
		clusterID: clusterID,
	}, nil
}


type clientClusterManager struct {
	cm *ClusterManager
	cs *client.Session
	lock sync.Mutex
	clusterID uint64
	tickTimer *time.Timer
	tickerLock sync.Mutex
}

func (c *clientClusterManager) nodeStarted(clusterName string, clusterID uint64, nodeID int, numNodes int, numGroups int,
	replicationFactor int, windowSizeTicks int, minTicksInWindow int) (bool, error) {
	if c.clusterID != clusterID {
		return false, errors.Errorf("attempt to register cluster %cm with different cluster id %d", clusterName, clusterID)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	command := &nodeStartedCommand{
		nodeID:            nodeID,
		numNodes:          numNodes,
		numGroups:         numGroups,
		replicationFactor: replicationFactor,
		windowSizeTicks:   windowSizeTicks,
		minTicksInWindow:  minTicksInWindow,
	}
	bytes := command.serialize()
	res, err := c.cm.nodeHost.SyncPropose(context.Background(), c.cs, bytes)
	if err != nil {
		return false, err
	}
	// TODO handle error return from SM
	ok := res.Value != 0
	return ok, nil
}

func (c *clientClusterManager) nodeStopped(nodeID int) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	command := &nodeStoppedCommand{nodeID: nodeID}
	bytes := command.serialize()
	_, err := c.cm.nodeHost.SyncPropose(context.Background(), c.cs, bytes)
	if err != nil {
		return err
	}
	// TODO handle error return from SM
	return nil
}

func (c *clientClusterManager) getClusterState(clusterName string, nodeID int, version uint64) (*ClusterState, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	command := &getClusterStateCommand{
		clusterName: clusterName,
		nodeID:      nodeID,
		version:     version,
	}
	bytes := command.serialize()
	res, err := c.cm.nodeHost.SyncPropose(context.Background(), c.cs, bytes)
	if err != nil {
		return nil, err
	}
	// TODO handle error return from SM
	state := &ClusterState{}
	state.deserialize(res.Data)
	return state, nil
}

func (c *clientClusterManager) clockTick() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, err := c.cm.nodeHost.SyncPropose(context.Background(), c.cs, []byte{clockTickCommandID})
	return err
}

func (c *clientClusterManager) setLeader(nodeID int, term uint64) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	command := &setLeaderCommand{
		leaderID: nodeID,
		term: term,
	}
	bytes := command.serialize()
	_, err := c.cm.nodeHost.SyncPropose(context.Background(), c.cs, bytes)
	return err
}

func (c *clientClusterManager) startTicker() {
	c.tickerLock.Lock()
	defer c.tickerLock.Unlock()
	c.startTickerNoLock()
}

func (c *clientClusterManager) startTickerNoLock() {
	if c.tickTimer != nil {
		panic("tick timer already running")
	}
	c.tickTimer = time.AfterFunc(c.cm.conf.TickTimerPeriod, func() {
		c.tickerLock.Lock()
		defer c.tickerLock.Unlock()
		if c.tickTimer == nil {
			return
		}
		if err := c.clockTick(); err != nil {
			log.Errorf("failed to send clock tick %v", err)
			return
		}
		c.startTickerNoLock()
	})
}

func (c *clientClusterManager) stopTicker() {
	c.tickerLock.Lock()
	defer c.tickerLock.Unlock()
	if c.tickTimer == nil {
		panic("tick timer not running")
	}
	c.tickTimer.Stop()
	c.tickTimer = nil
}

type nodeStartedCommand struct {
	nodeID int
	numNodes int
	numGroups int
	replicationFactor int
	windowSizeTicks int
	minTicksInWindow int
}

func (n *nodeStartedCommand) serialize() []byte {
	bytes := []byte{nodeStartedCommandID}
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(n.nodeID))
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(n.numNodes))
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(n.numGroups))
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(n.replicationFactor))
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(n.windowSizeTicks))
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(n.minTicksInWindow))
	return bytes
}

func (n *nodeStartedCommand) deserialize(bytes []byte) {
	offset := 0
	var nodeID uint32
	nodeID, offset = common.ReadUint32FromBufferLE(bytes, offset)
	n.nodeID = int(nodeID)
	var numNodes uint32
	numNodes, offset = common.ReadUint32FromBufferLE(bytes, offset)
	n.numNodes = int(numNodes)
	var numGroups uint32
	numGroups, offset = common.ReadUint32FromBufferLE(bytes, offset)
	n.numGroups = int(numGroups)
	var replicationFactor uint32
	replicationFactor, offset = common.ReadUint32FromBufferLE(bytes, offset)
	n.replicationFactor = int(replicationFactor)
	var windowSizeTicks uint32
	windowSizeTicks, offset = common.ReadUint32FromBufferLE(bytes, offset)
	n.windowSizeTicks = int(windowSizeTicks)
	var minTicksInWindow uint32
	minTicksInWindow, offset = common.ReadUint32FromBufferLE(bytes, offset)
	n.minTicksInWindow = int(minTicksInWindow)
}

type nodeStoppedCommand struct {
	nodeID int
}

func (n *nodeStoppedCommand) serialize() []byte {
	bytes := []byte{nodeStoppedCommandID}
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(n.nodeID))
	return bytes
}

func (n *nodeStoppedCommand) deserialize(bytes []byte) {
	nodeID, _ := common.ReadUint32FromBufferLE(bytes, 0)
	n.nodeID = int(nodeID)
}

type getClusterStateCommand struct {
	clusterName string
	nodeID int
	version uint64
}

func (g *getClusterStateCommand) serialize() []byte {
	bytes := []byte{getClusterStateCommandID}
	bytes = common.AppendStringToBufferLE(bytes, g.clusterName)
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(g.nodeID))
	bytes = common.AppendUint64ToBufferLE(bytes, g.version)
	return bytes
}

func (g *getClusterStateCommand) deserialize(bytes []byte) {
	offset := 0
	var clusterName string
	clusterName, offset = common.ReadStringFromBufferLE(bytes, offset)
	g.clusterName = clusterName
	var nodeID uint32
	nodeID, offset = common.ReadUint32FromBufferLE(bytes, offset)
	g.nodeID = int(nodeID)
	g.version, offset = common.ReadUint64FromBufferLE(bytes, offset)
}

type setLeaderCommand struct {
	leaderID int
	term uint64
}

func (s *setLeaderCommand) serialize() []byte {
	bytes := []byte{setLeaderCommandID}
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(s.leaderID))
	bytes = common.AppendUint64ToBufferLE(bytes, s.term)
	return bytes
}

func (s *setLeaderCommand) deserialize(bytes []byte) {
	lid, offset := common.ReadUint32FromBufferLE(bytes, 0)
	term, offset := common.ReadUint64FromBufferLE(bytes, offset)
	s.leaderID = int(lid)
	s.term = term
}

