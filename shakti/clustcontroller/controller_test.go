package clustcontroller

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common/commontest"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"github.com/squareup/pranadb/remoting"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"sync"
	"testing"
	"time"
)

var clusterName = "test_clust"
var tickTimerPeriod = 500 * time.Millisecond
var defaultWindowSizeTicks = 5
var defaultWaitTimeout = 3 * tickTimerPeriod * time.Duration(defaultWindowSizeTicks)
var defaultClusterControllerClusterSize = 3

/*
TDOO;
1. Test all errors
3. Test multiple prana clusters
*/

func TestErrors(t *testing.T) {
	client, tups, apiServerListenAddresses := setup(t, defaultClusterControllerClusterSize)
	defer tearDown(t, client, tups)

	message := &clustermsgs.NodeStartedMessage{
		ClusterName:      clusterName,
		ClusterId:        2,
		NodeId:           -1,
		NumNodes:         3,
		NumGroups:        10,
		MinReplicas:      2,
		MaxReplicas:      3,
		WindowSizeTicks:  5,
		MinTicksInWindow: 1,
	}
	resp, err := client.SendRPC(message, apiServerListenAddresses[0])
	require.Error(t, err)
	require.Nil(t, resp)

	// TODO test all the errors
}

func TestLoseControllerNodes(t *testing.T) {

	client, tups, apiServerListenAddresses := setup(t, 5)
	defer tearDown(t, client, tups)

	// Start a Prana cluster
	sendNodeStarted(t, client, apiServerListenAddresses, clusterName, 2, 5, 10,
		3, 3, defaultWindowSizeTicks, 1, 0, 1, 2, 3, 4)

	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddresses,
		3, 1, client, tups, 0, 1, 2, 3, 4)

	// Now we stop 1 cluster controller node - this time a non leader
	leaderNode, err := getLeaderNode(clusterName, tups)
	require.NoError(t, err)
	nodeID := (leaderNode + 1) % 5

	err = tups[nodeID].server.Stop()
	require.NoError(t, err)
	err = tups[nodeID].controller.Stop()
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Cluster state should be the same
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddresses,
		3, 1, client, tups, 0, 1, 2, 3, 4)

	// Now stop another node we should still have a quorum, so everything should be ok still
	// This time we will make sure we stop the leader
	leaderNode, err = getLeaderNode(clusterName, tups)
	require.NoError(t, err)
	require.NotEqual(t, -1, leaderNode)
	err = tups[leaderNode].server.Stop()
	require.NoError(t, err)
	err = tups[leaderNode].controller.Stop()
	require.NoError(t, err)

	// Wait until the leader node changes
	ok, err := commontest.WaitUntilWithError(func() (bool, error) {
		ln, err := getLeaderNode(clusterName, tups)
		if err != nil {
			return false, err
		}
		return ln != -1 && ln != leaderNode, nil
	}, 10*time.Second, 100*time.Millisecond)
	require.True(t, ok)
	require.NoError(t, err)

	// Cluster state should be the same
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddresses,
		3, 1, client, tups, 0, 1, 2, 3, 4)

	// Now we lose another node. We stop the leader again. This means there will be no quorum
	leaderNode, err = getLeaderNode(clusterName, tups)
	require.NoError(t, err)
	require.NotEqual(t, -1, leaderNode)
	err = tups[leaderNode].server.Stop()
	require.NoError(t, err)
	err = tups[leaderNode].controller.Stop()
	require.NoError(t, err)

	// Wait until leader node == -1 (no leader)
	ok, err = commontest.WaitUntilWithError(func() (bool, error) {
		ln, err := getLeaderNode(clusterName, tups)
		if err != nil {
			return false, err
		}
		return ln == -1, nil
	}, 10*time.Second, 100*time.Millisecond)
	require.True(t, ok)
	require.NoError(t, err)

	// Start a cluster controller async, which will mean the cluster controller cluster has a quorum again
	// and the next line will succeed

	go func() {
		time.Sleep(1 * time.Second)
		err := tups[leaderNode].controller.Start()
		if err != nil {
			panic(err)
		}
	}()

	// Now we have no quorum - this will retry until another cluster controller node comed back up
	// We wait longer this time as it requires leader election and can take a little while
	waitUntilNodesGetState(t, 20*time.Second, tickTimerPeriod, apiServerListenAddresses,
		3, 1, client, tups, 0, 1, 2, 3, 4)
}

func TestInsufficientNodesJoinClusterToReceiveState(t *testing.T) {
	client, tups, apiServerListenAddress := setup(t, defaultClusterControllerClusterSize)
	defer tearDown(t, client, tups)

	// Just start 2 nodes out of the cluster of size 5
	sendNodeStarted(t, client, apiServerListenAddress, clusterName, 2, 5, 10,
		3, 3, defaultWindowSizeTicks, 1, 2, 3)

	// We do not have min replicas so they shouldn't receive any state
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		0, -1, client, tups, 2, 3)
}

func TestMinReplicasNodesJoinAndReceiveState(t *testing.T) {
	client, tups, apiServerListenAddress := setup(t, defaultClusterControllerClusterSize)
	defer tearDown(t, client, tups)

	// Just start 3 nodes out of the cluster of size 5
	sendNodeStarted(t, client, apiServerListenAddress, clusterName, 2, 5, 10,
		3, 3, defaultWindowSizeTicks, 1, 1, 3, 4)

	// We have min replicas so they should receive state
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		3, 1, client, tups, 1, 3, 4)
}

func TestMoreThanMinReplicasNodesJoinAndReceiveState(t *testing.T) {
	client, tups, apiServerListenAddress := setup(t, defaultClusterControllerClusterSize)
	defer tearDown(t, client, tups)

	// Start 3 nodes out of the cluster of size 5, also we set max replicas to 5
	sendNodeStarted(t, client, apiServerListenAddress, clusterName, 2, 5, 10,
		3, 5, defaultWindowSizeTicks, 1, 1, 2, 3, 4)

	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		4, 1, client, tups, 1, 2, 3, 4)
}

func TestAllNodesJoinAndReceiveState(t *testing.T) {
	client, tups, apiServerListenAddress := setup(t, defaultClusterControllerClusterSize)
	defer tearDown(t, client, tups)

	// Start all nodes
	sendNodeStarted(t, client, apiServerListenAddress, clusterName, 2, 5, 10,
		3, 3, defaultWindowSizeTicks, 1, 0, 1, 2, 3, 4)

	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		3, 1, client, tups, 0, 1, 2, 3, 4)
}

func TestAllNodesJoinAndReceiveStateClusterSizeEqualsMinReplicas(t *testing.T) {
	clusterSize := 5
	client, tups, apiServerListenAddress := setup(t, defaultClusterControllerClusterSize)
	defer tearDown(t, client, tups)

	// Start all nodes
	sendNodeStarted(t, client, apiServerListenAddress, clusterName, 2, clusterSize, 10,
		clusterSize, clusterSize, defaultWindowSizeTicks, 1, 0, 1, 2, 3, 4)

	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		clusterSize, 1, client, tups, 0, 1, 2, 3, 4)
}

func TestNodesExpireThenRejoin(t *testing.T) {
	client, tups, apiServerListenAddress := setup(t, defaultClusterControllerClusterSize)
	defer tearDown(t, client, tups)

	sendNodeStarted(t, client, apiServerListenAddress, clusterName, 2, 5, 10,
		2, 4, defaultWindowSizeTicks, 1, 0, 1, 2, 3, 4)

	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		4, 1, client, tups, 0, 1, 2, 3, 4)

	// Now we expire a node (node 2)
	// And make sure we see the next version and groups without the expired node
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		4, 2, client, tups, 0, 1, 3, 4)

	// And expire another one - we now have fewer then max replicas
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		3, 3, client, tups, 1, 3, 4)

	// Expire one more-  now we have minReplicas nodes
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		2, 4, client, tups, 1, 4)

	// Expire one more-  now we less than min replicas nodes, so no cluster state should be returned
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		0, -1, client, tups, 4)

	// Now add a node back - now we should heva min replicas
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		2, 5, client, tups, 1, 3)

	// Now add a node back
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		3, 6, client, tups, 1, 3, 2)

	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		4, 7, client, tups, 1, 3, 2, 4)

	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		4, 8, client, tups, 1, 3, 2, 4, 0)
}

func TestNodeStopRemovesFromClusterImmediately(t *testing.T) {
	client, tups, apiServerListenAddress := setup(t, defaultClusterControllerClusterSize)
	defer tearDown(t, client, tups)

	// Start all nodes
	sendNodeStarted(t, client, apiServerListenAddress, clusterName, 2, 5, 10,
		3, 3, defaultWindowSizeTicks, 1, 0, 1, 2, 3, 4)

	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		3, 1, client, tups, 0, 1, 2, 3, 4)

	sendNodeStopped(t, client, apiServerListenAddress, clusterName, 2)

	start := time.Now()
	waitUntilNodesGetState(t, defaultWaitTimeout, tickTimerPeriod, apiServerListenAddress,
		3, 2, client, tups, 0, 1, 3, 4)
	require.Less(t, time.Now().Sub(start), time.Second)
}

func genAPIServerListenAddresses(numNodes int) []string {
	apiListenAddresses := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		apiListenAddresses[i] = fmt.Sprintf("127.0.0.1:%d", i+63301)
	}
	return apiListenAddresses
}

type serverTup struct {
	server     *Server
	controller *ClusterController
}

func startClusterControllerCluster(t *testing.T, apiServerListenAddresses []string, tickTimerPeriod time.Duration, numNodes int) []serverTup {
	t.Helper()
	dataDirBase, err := ioutil.TempDir("", "clust_controller_test")
	if err != nil {
		panic("failed to create temp dir")
	}
	raftListenAddresses := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		raftListenAddresses[i] = fmt.Sprintf("127.0.0.1:%d", i+63201)
	}
	chans := make([]chan error, numNodes)
	var tups []serverTup
	for i := 0; i < numNodes; i++ {
		ch := make(chan error, 1)
		chans[i] = ch
		config := Config{
			ClusterManagerClusterID: 1,
			NodeID:                  i,
			DataDir:                 fmt.Sprintf("%s-%d", dataDirBase, i),
			RaftListenAddresses:     raftListenAddresses,
			Clusters: []ClusterConfig{{
				ClusterName: clusterName,
				ClusterID:   2,
			}},
			RaftRTTMs:              50,
			RaftElectionRTT:        100,
			RaftHeartbeatRTT:       10,
			RaftMetricsEnabled:     false,
			RaftSnapshotEntries:    1000,
			RaftCompactionOverhead: 50,
			IntraClusterTLSConfig:  conf.TLSConfig{},
			TickTimerPeriod:        tickTimerPeriod,
		}
		controller := NewClusterController(config)
		server := NewServer(apiServerListenAddresses[i], conf.TLSConfig{}, controller)
		tups = append(tups, serverTup{
			server:     server,
			controller: controller,
		})
		go func() {
			// Start them in parallel
			err := controller.Start()
			if err != nil {
				ch <- err
				return
			}
			err = server.Start()
			ch <- err
		}()
	}
	for _, ch := range chans {
		err := <-ch
		require.NoError(t, err)
	}
	return tups
}

func waitUntilNodesGetState(t *testing.T, timeout time.Duration, interval time.Duration, apiListenAddresses []string,
	requiredReplicas int, requireVersion int, client *remoting.Client, tups []serverTup, nodeIDs ...int) {
	t.Helper()
	// Whittle down to the started servers
	var startedServers []string
	for i, address := range apiListenAddresses {
		if tups[i].controller.IsStarted() {
			startedServers = append(startedServers, address)
		}
	}
	numGetters := len(nodeIDs)
	getters := make([]*stateGetter, numGetters)
	for i, nodeID := range nodeIDs {
		// Doesn't matter which server we choose as long as it's started
		serverAddress := startedServers[nodeID%len(startedServers)]
		getters[i] = &stateGetter{
			nodeID:           nodeID,
			timeout:          timeout,
			interval:         interval,
			apiListenAddress: serverAddress,
			requiredReplicas: requiredReplicas,
			requiredVersion:  requireVersion,
			client:           client,
		}
		getters[i].start()
	}
	for i := 0; i < numGetters; i++ {
		cs := getters[i].wait()
		if requiredReplicas == 0 {
			require.Nil(t, cs)
		} else {
			require.NotNil(t, cs)
			verifyClusterState(t, cs, 10, requiredReplicas, nodeIDs)
		}
	}
}

func setup(t *testing.T, clusterControllerClusterSize int) (*remoting.Client, []serverTup, []string) {
	t.Helper()
	apiServerListenAddress := genAPIServerListenAddresses(clusterControllerClusterSize)
	tups := startClusterControllerCluster(t, apiServerListenAddress, tickTimerPeriod, clusterControllerClusterSize)
	client, err := remoting.NewClient(conf.TLSConfig{})
	require.NoError(t, err)
	return client, tups, apiServerListenAddress
}

func tearDown(t *testing.T, client *remoting.Client, tups []serverTup) {
	t.Helper()
	client.Stop()
	for _, tup := range tups {
		err := tup.server.Stop()
		require.NoError(t, err)
		err = tup.controller.Stop()
		require.NoError(t, err)
	}
}

type stateGetter struct {
	nodeID           int
	timeout          time.Duration
	interval         time.Duration
	apiListenAddress string
	client           *remoting.Client
	requiredReplicas int
	requiredVersion  int
	wg               sync.WaitGroup
	cs               *ClusterState
}

func (d *stateGetter) start() {
	d.wg.Add(1)
	go d.runLoop()
}

func (d *stateGetter) wait() *ClusterState {
	d.wg.Wait()
	return d.cs
}

// we wait for requiredReplicas replicas to be in the state or timeout
func (d *stateGetter) runLoop() {
	start := time.Now()
	prevVersion := 0
	for time.Now().Sub(start) < d.timeout {
		getClusterMessage := &clustermsgs.GetClusterStateMessage{
			ClusterName: clusterName,
			NodeId:      int32(d.nodeID),
			Version:     int64(prevVersion),
		}
		resp, err := d.client.SendRPC(getClusterMessage, d.apiListenAddress)
		if err != nil {
			panic(err)
		}
		if resp == nil {
			panic("resp is nil")
		}
		getClusterResp := resp.(*clustermsgs.GetClusterStateResponse) //nolint:forcetypeassert
		var cs *ClusterState
		if getClusterResp.Payload != nil {
			cs = &ClusterState{}
			cs.deserialize(getClusterResp.Payload, 0)
			prevVersion = int(cs.Version)
			numReps := len(cs.GroupNodes[0])
			log.Debugf("node %d received state %v", d.nodeID, cs)
			if numReps == d.requiredReplicas && int(cs.Version) == d.requiredVersion {
				d.cs = cs
				break
			}
		}
		time.Sleep(d.interval)
	}
	d.wg.Done()
}

//nolint:unparam
func sendNodeStarted(t *testing.T, client *remoting.Client, apiListenAddresses []string, clusterName string,
	clusterID int, clusterSize int, numGroups int, minReplicas int, maxReplicas int, windowSizeTicks int,
	minTicksInWindow int, nodeIDs ...int) {
	t.Helper()
	message := &clustermsgs.NodeStartedMessage{
		ClusterName:      clusterName,
		ClusterId:        int64(clusterID),
		NumNodes:         int32(clusterSize),
		NumGroups:        int32(numGroups),
		MinReplicas:      int32(minReplicas),
		MaxReplicas:      int32(maxReplicas),
		WindowSizeTicks:  int32(windowSizeTicks),
		MinTicksInWindow: int32(minTicksInWindow),
	}
	for i := 0; i < len(nodeIDs); i++ {
		message.NodeId = int32(i)
		// Round robin the cluster controller node we will talk to
		ccNode := i % len(apiListenAddresses)
		resp, err := client.SendRPC(message, apiListenAddresses[ccNode])
		require.NoError(t, err)
		require.Nil(t, resp)
	}
}

func sendNodeStopped(t *testing.T, client *remoting.Client, apiListenAddresses []string, clusterName string, nodeIDs ...int) {
	t.Helper()
	message := &clustermsgs.NodeStoppedMessage{
		ClusterName: clusterName,
	}
	for i, nodeID := range nodeIDs {
		message.NodeId = int32(nodeID)
		// Round robin the cluster controller node we will talk to
		ccNode := i % len(apiListenAddresses)
		resp, err := client.SendRPC(message, apiListenAddresses[ccNode])
		require.NoError(t, err)
		require.Nil(t, resp)
	}
}

func getLeaderNode(clusterName string, tups []serverTup) (int, error) {
	for i, tup := range tups {
		isLeader, err := tup.controller.IsLeader(clusterName)
		if err != nil {
			return -1, err
		}
		if isLeader {
			return i, nil
		}
	}
	return -1, nil
}
