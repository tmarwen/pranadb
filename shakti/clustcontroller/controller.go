package clustcontroller

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	sm "github.com/lni/dragonboat/v3/statemachine"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"github.com/squareup/pranadb/remoting"
	"path/filepath"
	"time"

	"sync"
)

type ClusterController struct {
	conf               Config
	nodeHost           *dragonboat.NodeHost
	clients            sync.Map
	clientsByClusterID sync.Map
	setLeaderChan      chan setLeaderInfo
	stopSignaller      common.AtomicBool
	started            bool
	startStopLock      sync.Mutex
}

type setLeaderInfo struct {
	ccm      *clientClusterManager
	leaderID int
	term     uint64
}

const (
	// cluster state machine command
	nodeStartedCommandID     = 1
	nodeStoppedCommandID     = 2
	getClusterStateCommandID = 3
	clockTickCommandID       = 4
	setLeaderCommandID       = 5
)

const raftCallTimeout = 5 * time.Second
const operationRetryTimeout = 1 * time.Hour

type ClusterConfig struct {
	ClusterName string
	ClusterID   uint64
}

type Config struct {
	ClusterManagerClusterID uint64
	NodeID                  int
	DataDir                 string
	RaftListenAddresses     []string
	Clusters                []ClusterConfig
	RaftRTTMs               uint64
	RaftElectionRTT         uint64
	RaftHeartbeatRTT        uint64
	RaftMetricsEnabled      bool
	RaftSnapshotEntries     uint64
	RaftCompactionOverhead  uint64
	IntraClusterTLSConfig   conf.TLSConfig
	TickTimerPeriod         time.Duration
}

func NewClusterController(conf Config) *ClusterController {
	// TODO validate config fully
	if conf.TickTimerPeriod == 0 {
		panic("tickTimerPeriod must be > 0")
	}
	return &ClusterController{
		conf:          conf,
		setLeaderChan: make(chan setLeaderInfo, 100),
	}
}

func (cm *ClusterController) Start() error {
	cm.startStopLock.Lock()
	defer cm.startStopLock.Unlock()
	if cm.started {
		return nil
	}

	// Screen out the log spam
	logger.GetLogger("rsm").SetLevel(logger.INFO)
	logger.GetLogger("transport").SetLevel(logger.ERROR)
	logger.GetLogger("grpc").SetLevel(logger.ERROR)
	logger.GetLogger("raft").SetLevel(logger.INFO)
	logger.GetLogger("dragonboat").SetLevel(logger.ERROR)

	// Start the node host
	nodeAddress := cm.conf.RaftListenAddresses[cm.conf.NodeID]
	dragonBoatDir := filepath.Join(cm.conf.DataDir, "raft")
	nhc := config.NodeHostConfig{
		DeploymentID:      cm.conf.ClusterManagerClusterID,
		WALDir:            dragonBoatDir,
		NodeHostDir:       dragonBoatDir,
		RTTMillisecond:    cm.conf.RaftRTTMs,
		RaftAddress:       nodeAddress,
		EnableMetrics:     cm.conf.RaftMetricsEnabled,
		Expert:            config.GetDefaultExpertConfig(),
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
		return errors.WithStack(err)
	}
	cm.nodeHost = nh
	log.Debugf("created nodehost on node %d", cm.conf.NodeID)
	// Every node is a member of the raft group
	initialMembers := make(map[uint64]string)
	for nid, listenAddress := range cm.conf.RaftListenAddresses {
		initialMembers[uint64(nid)+1] = listenAddress
	}
	// We start the cluster raft groups in parallel
	var chans []chan error
	for _, cc := range cm.conf.Clusters {
		// Start the state machines
		rc := createRaftGroupConfig(cm.conf.NodeID, cc.ClusterID, cm.conf)
		ch := make(chan error, 1)
		chans = append(chans, ch)
		clusterName := cc.ClusterName
		//go func() {
		factory := func(_ uint64, _ uint64) sm.IStateMachine {
			csm := newClusterStateMachine(cm)
			csm.state.thisNodeID = cm.conf.NodeID
			csm.state.leaderID = -1
			csm.state.clusterName = clusterName
			return csm
		}
		//ch <- nh.StartCluster(initialMembers, false, factory, rc)
		if err := nh.StartCluster(initialMembers, false, factory, rc); err != nil {
			return errors.WithStack(err)
		}
		//}()
	}
	log.Debugf("waiting for cluster state machine to start on node %d", cm.conf.NodeID)
	//for _, ch := range chans {
	//	err := <-ch
	//	if err != nil {
	//		return errors.WithStack(err)
	//	}
	//}
	log.Debugf("cluster state machine started on node %d", cm.conf.NodeID)
	for _, cc := range cm.conf.Clusters {
		ccm := cm.newClientClusterManager(cc.ClusterID)
		cm.clients.Store(cc.ClusterName, ccm)
		cm.clientsByClusterID.Store(cc.ClusterID, ccm)
	}
	go cm.setLeaderLoop()
	cm.started = true
	return nil
}

func (cm *ClusterController) Stop() error {
	cm.startStopLock.Lock()
	defer cm.startStopLock.Unlock()
	if !cm.started {
		return nil
	}
	cm.stopSignaller.Set(true)
	cm.clients.Range(func(_, value interface{}) bool {
		ccm := value.(*clientClusterManager) //nolint:forcetypeassert
		ccm.stopTicker()
		return true
	})
	cm.nodeHost.Stop()
	close(cm.setLeaderChan)
	cm.started = false
	// Reset state because this can be restarted
	cm.nodeHost = nil
	cm.clients = sync.Map{}
	cm.clientsByClusterID = sync.Map{}
	cm.stopSignaller = common.AtomicBool{}
	cm.setLeaderChan = make(chan setLeaderInfo, 100)
	return nil
}

func createRaftGroupConfig(nodeID int, clusterID uint64, conf Config) config.Config {
	return config.Config{
		NodeID:             uint64(nodeID + 1),
		ElectionRTT:        conf.RaftElectionRTT,
		HeartbeatRTT:       conf.RaftHeartbeatRTT,
		CheckQuorum:        true,
		SnapshotEntries:    conf.RaftSnapshotEntries,
		CompactionOverhead: conf.RaftCompactionOverhead,
		ClusterID:          clusterID,
	}
}

func (cm *ClusterController) becomeLeader(clusterName string, leader bool) error {
	c, err := cm.getClient(clusterName)
	if err != nil {
		return err
	}
	c.becomeLeader(leader)
	return nil
}

func (cm *ClusterController) NodeStarted(message *clustermsgs.NodeStartedMessage) error {
	c, err := cm.getClient(message.ClusterName)
	if err != nil {
		return err
	}
	return c.nodeStarted(message)
}

func (cm *ClusterController) NodeStopped(message *clustermsgs.NodeStoppedMessage) error {
	c, err := cm.getClient(message.ClusterName)
	if err != nil {
		return err
	}
	return c.nodeStopped(message)
}

func (cm *ClusterController) GetClusterState(message *clustermsgs.GetClusterStateMessage) ([]byte, error) {
	c, err := cm.getClient(message.ClusterName)
	if err != nil {
		return nil, err
	}
	return c.getClusterState(message)
}

func (cm *ClusterController) getClient(clusterName string) (*clientClusterManager, error) {
	o, ok := cm.clients.Load(clusterName)
	if !ok {
		// All clusters must be configured in the config file
		return nil, errors.Errorf("unknown cluster %s has it been configured in the cluster manager config?", clusterName)
	}
	return o.(*clientClusterManager), nil
}

func (cm *ClusterController) LeaderUpdated(info raftio.LeaderInfo) {
	if info.NodeID != uint64(cm.conf.NodeID)+1 {
		panic("received leader info on wrong node")
	}
	newLeaderID := int(info.LeaderID - 1)
	if newLeaderID == cm.conf.NodeID {
		// If the current node has become leader of a group we call setLeader
		o, ok := cm.clientsByClusterID.Load(info.ClusterID)
		if !ok {
			// All clusters must be configured in the config file
			log.Errorf("unknown cluster id %d", info.ClusterID)
		}
		ccm := o.(*clientClusterManager) //nolint:forcetypeassert
		cm.setLeaderChan <- setLeaderInfo{
			ccm:      ccm,
			leaderID: newLeaderID,
			term:     info.Term,
		}
	}
}

func (cm *ClusterController) IsLeader(clusterName string) (bool, error) {
	cm.startStopLock.Lock()
	defer cm.startStopLock.Unlock()
	if !cm.started {
		return false, nil
	}
	c, err := cm.getClient(clusterName)
	if err != nil {
		return false, err
	}
	return c.isLeader(), nil
}

func (cm *ClusterController) IsStarted() bool {
	cm.startStopLock.Lock()
	defer cm.startStopLock.Unlock()
	return cm.started
}

func (cm *ClusterController) setLeaderLoop() {
	for setLeaderInfo := range cm.setLeaderChan {
		if err := setLeaderInfo.ccm.setLeader(setLeaderInfo.leaderID, setLeaderInfo.term); err != nil {
			log.Errorf("failed to set leader %v", err)
		}
	}
}

func (cm *ClusterController) newClientClusterManager(clusterID uint64) *clientClusterManager {
	return &clientClusterManager{
		cm:        cm,
		clusterID: clusterID,
	}
}

type clientClusterManager struct {
	cm         *ClusterController
	cs         *client.Session
	lock       sync.Mutex
	clusterID  uint64
	leader     bool
	tickTimer  *time.Timer
	tickerLock sync.Mutex
}

func (c *clientClusterManager) isLeader() bool {
	c.tickerLock.Lock()
	defer c.tickerLock.Unlock()
	return c.leader
}

func (c *clientClusterManager) getSession() (*client.Session, error) {
	if c.cs != nil {
		return c.cs, nil
	}
	res, err := c.executeRaftOpWithRetry(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), raftCallTimeout)
		defer cancel()
		return c.cm.nodeHost.SyncGetSession(ctx, c.clusterID)
	}, operationRetryTimeout)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sess := res.(*client.Session) //nolint:forcetypeassert
	c.cs = sess
	return sess, nil
}

func (c *clientClusterManager) nodeStarted(message *clustermsgs.NodeStartedMessage) error {
	if c.clusterID != uint64(message.ClusterId) {
		return errors.Errorf("attempt to register cluster %s with different cluster id %d", message.ClusterName, message.ClusterId)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	var buff []byte
	buff = append(buff, nodeStartedCommandID)
	bytes, err := serializeMessage(buff, message)
	if err != nil {
		return err
	}
	_, err = c.executeProposeWithRetry(bytes)
	return err
}

func extractSMError(res sm.Result) error {
	ok := res.Data[0] == 1
	if !ok {
		errMsg, _ := common.ReadStringFromBufferLE(res.Data, 1)
		return errors.New(errMsg)
	}
	return nil
}

func (c *clientClusterManager) nodeStopped(message *clustermsgs.NodeStoppedMessage) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	var buff []byte
	buff = append(buff, nodeStoppedCommandID)
	bytes, err := serializeMessage(buff, message)
	if err != nil {
		return err
	}
	_, err = c.executeProposeWithRetry(bytes)
	return err
}

func (c *clientClusterManager) getClusterState(message *clustermsgs.GetClusterStateMessage) ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	var buff []byte
	buff = append(buff, getClusterStateCommandID)
	bytes, err := serializeMessage(buff, message)
	if err != nil {
		return nil, err
	}
	return c.executeProposeWithRetry(bytes)
}

func (c *clientClusterManager) clockTick() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, err := c.executeProposeWithRetry([]byte{clockTickCommandID})
	return err
}

func (c *clientClusterManager) setLeader(nodeID int, term uint64) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Debugf("leader changed calling set leader %d term %d", nodeID, term)
	command := &setLeaderCommand{
		leaderID: nodeID,
		term:     term,
	}
	bytes := command.serialize()
	_, err := c.executeProposeWithRetry(bytes)
	log.Debug("Got return from setLeader propose")
	return err
}

func (c *clientClusterManager) becomeLeader(leader bool) {
	c.tickerLock.Lock()
	defer c.tickerLock.Unlock()
	if leader {
		c.startTicker()
	} else {
		c.stopTicker()
	}
	c.leader = leader
}

func (c *clientClusterManager) startTicker() {
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
		c.tickTimer = nil
		c.startTicker()
	})
}

func (c *clientClusterManager) stopTicker() {
	if c.tickTimer == nil {
		return
	}
	c.tickTimer.Stop()
	c.tickTimer = nil
}

type setLeaderCommand struct {
	leaderID int
	term     uint64
}

func (s *setLeaderCommand) serialize() []byte {
	bytes := []byte{setLeaderCommandID}
	bytes = common.AppendUint32ToBufferLE(bytes, uint32(s.leaderID))
	bytes = common.AppendUint64ToBufferLE(bytes, s.term)
	return bytes
}

func (s *setLeaderCommand) deserialize(bytes []byte) {
	lid, offset := common.ReadUint32FromBufferLE(bytes, 0)
	term, _ := common.ReadUint64FromBufferLE(bytes, offset)
	s.leaderID = int(lid)
	s.term = term
}

func serializeMessage(buff []byte, message remoting.ClusterMessage) ([]byte, error) {
	b := proto.NewBuffer(buff)
	if err := b.Marshal(message); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (c *clientClusterManager) executeProposeWithRetry(request []byte) ([]byte, error) {
	cs, err := c.getSession()
	if err != nil {
		return nil, err
	}
	res, err := c.executeRaftOpWithRetry(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), raftCallTimeout)
		defer cancel()
		res, err := c.cm.nodeHost.SyncPropose(ctx, cs, request)
		if err == nil {
			cs.ProposalCompleted()
		}
		return res, err
	}, operationRetryTimeout)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	smr := res.(sm.Result)    //nolint:forcetypeassert
	err = extractSMError(smr) //nolint:forcetypeassert
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return smr.Data[1:], nil //nolint:forcetypeassert
}

// Some errors are expected, we should retry in this case
// See https://github.com/lni/dragonboat/issues/183
func (c *clientClusterManager) executeRaftOpWithRetry(f func() (interface{}, error), timeout time.Duration) (interface{}, error) {
	start := time.Now()
	for {
		res, err := f()
		if err == nil {
			return res, nil
		}
		// These errors are to be expected and transient
		// ErrClusterNotReady if there is no leader - e.g. election in progress or no quorum (node lost or startup)
		// ErrTimeout - can occur if the operation takes too long to process - e.g. when under load
		// The others - typically when the cluster is still starting up or closing down and the raft groups are
		// We retry in this case
		if !errors.Is(err, dragonboat.ErrClusterNotReady) && !errors.Is(err, dragonboat.ErrTimeout) &&
			!errors.Is(err, dragonboat.ErrClusterNotFound) && !errors.Is(err, dragonboat.ErrClusterClosed) &&
			!errors.Is(err, dragonboat.ErrClusterNotInitialized) && !errors.Is(err, dragonboat.ErrClusterNotBootstrapped) &&
			!errors.Is(err, dragonboat.ErrSystemBusy) && !errors.Is(err, dragonboat.ErrInvalidDeadline) &&
			!errors.Is(err, dragonboat.ErrClosed) {
			return nil, errors.WithStack(err)
		}
		if time.Now().Sub(start) >= timeout {
			// If we timeout, then something is seriously wrong
			log.Errorf("error in making dragonboat calls %+v", err)
			return nil, err
		}
		if c.cm.stopSignaller.Get() {
			// Server is closing - break out of retry loop
			return nil, errors.New("server is closing")
		}
		log.Debugf("Received retryable error in raft operation %v, will retry after delay", err)
		time.Sleep(500 * time.Millisecond)
	}
}
