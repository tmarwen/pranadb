package clustcontroller

import (
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/protos/squareup/cash/pranadb/v1/clustermsgs"
	"github.com/squareup/pranadb/remoting"
)

// Server is the remoting server for the cluster manager
type Server struct {
	rServer remoting.Server
}

func NewServer(listenAddress string, tlsConf conf.TLSConfig, clusterManager *ClusterController) *Server {
	rServer := remoting.NewServer(listenAddress, tlsConf)
	rServer.RegisterMessageHandler(remoting.ClusterMessageClusterControllerNodeStartedMessage, &NodeStartedHandler{clusterManager: clusterManager})
	rServer.RegisterMessageHandler(remoting.ClusterMessageClusterControllerNodeStoppedMessage, &NodeStoppedHandler{clusterManager: clusterManager})
	rServer.RegisterMessageHandler(remoting.ClusterMessageClusterControllerGetClusterStateMessage, &GetClusterStateHandler{clusterManager: clusterManager})
	return &Server{
		rServer: rServer,
	}
}

func (s *Server) Start() error {
	return s.rServer.Start()
}

func (s *Server) Stop() error {
	return s.rServer.Stop()
}

type NodeStartedHandler struct {
	clusterManager *ClusterController
}

func (n *NodeStartedHandler) HandleMessage(clusterMessage remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	nsm := clusterMessage.(*clustermsgs.NodeStartedMessage) //nolint:forcetypeassert
	err := n.clusterManager.NodeStarted(nsm)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type NodeStoppedHandler struct {
	clusterManager *ClusterController
}

func (n *NodeStoppedHandler) HandleMessage(clusterMessage remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	nsm := clusterMessage.(*clustermsgs.NodeStoppedMessage) //nolint:forcetypeassert
	err := n.clusterManager.NodeStopped(nsm)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

type GetClusterStateHandler struct {
	clusterManager *ClusterController
}

func (n *GetClusterStateHandler) HandleMessage(clusterMessage remoting.ClusterMessage) (remoting.ClusterMessage, error) {
	gcsm := clusterMessage.(*clustermsgs.GetClusterStateMessage) //nolint:forcetypeassert
	bytes, err := n.clusterManager.GetClusterState(gcsm)
	if err != nil {
		return nil, err
	}
	return &clustermsgs.GetClusterStateResponse{Payload: bytes}, nil
}
