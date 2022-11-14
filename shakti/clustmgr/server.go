package clustmgr

import (
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/remoting"
)

// Server is the remoting server for the cluster manager
type Server struct {
	rServer remoting.Server
	clusterManager *ClusterManager
}

func NewServer(listenAddress string, tlsConf conf.TLSConfig, clusterManager *ClusterManager) *Server {
	rServer := remoting.NewServer(listenAddress, tlsConf)

	// FIXME - think about how to abstract out cluster messages a bit better so we can have our own here
	rServer.RegisterMessageHandler()
	
	return &Server{
		rServer: rServer,
		clusterManager: clusterManager,
	}
}

func (s *Server) Start() error {
	return s.rServer.Start()
}

func (s *Server) Stop() error {
	return s.rServer.Stop()
}