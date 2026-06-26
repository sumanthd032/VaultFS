// Package main is the entry point for a VaultFS master node.
//
// A master serves the MasterService and AdminService over mTLS and replicates
// its namespace through a Raft node. Configuration comes from flags, each with
// an environment-variable fallback, so the same binary runs under
// docker-compose. Run one master for a single-node setup or three with peer
// addresses for a fault-tolerant cluster.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/sumanthd032/vaultfs/internal/master"
	"github.com/sumanthd032/vaultfs/internal/metadata"
	"github.com/sumanthd032/vaultfs/internal/metrics"
	"github.com/sumanthd032/vaultfs/internal/raft"
	"github.com/sumanthd032/vaultfs/internal/security"
	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

type config struct {
	nodeID      string
	listen      string
	raftListen  string
	raftPeers   []string
	dataDir     string
	chunkNodes  []*vaultfsv1.NodeInfo
	replication int
	metricsAddr string
	certFile    string
	keyFile     string
	caFile      string
}

func envOr(env, def string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return def
}

// splitList splits a comma-separated list, dropping empty entries.
func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseChunkNodes parses "id@addr" pairs (or bare "addr") into NodeInfo values.
func parseChunkNodes(spec string) []*vaultfsv1.NodeInfo {
	var nodes []*vaultfsv1.NodeInfo
	for _, item := range splitList(spec) {
		id, addr := item, item
		if name, address, ok := strings.Cut(item, "@"); ok {
			id, addr = name, address
		}
		nodes = append(nodes, &vaultfsv1.NodeInfo{NodeId: id, Address: addr})
	}
	return nodes
}

func parseConfig() config {
	var c config
	var raftPeers, chunkServers string
	flag.StringVar(&c.nodeID, "node-id", envOr("VAULTFS_NODE_ID", "master-0"), "stable node identifier")
	flag.StringVar(&c.listen, "listen", envOr("VAULTFS_LISTEN", ":9000"), "client-facing gRPC address")
	flag.StringVar(&c.raftListen, "raft-listen", envOr("VAULTFS_RAFT_LISTEN", ":9200"), "Raft peer gRPC address")
	flag.StringVar(&raftPeers, "raft-peers", envOr("VAULTFS_RAFT_PEERS", ""), "comma-separated Raft addresses of other masters")
	flag.StringVar(&c.dataDir, "data-dir", envOr("VAULTFS_DATA_DIR", "/var/lib/vaultfs/meta"), "metadata directory")
	flag.StringVar(&chunkServers, "chunkservers", envOr("VAULTFS_CHUNKSERVERS", ""), "comma-separated chunk servers as id@addr")
	flag.IntVar(&c.replication, "replication", metadata.DefaultReplicationFactor, "replication factor")
	flag.StringVar(&c.metricsAddr, "metrics-addr", envOr("VAULTFS_METRICS_ADDR", ":9001"), "Prometheus metrics HTTP address")
	flag.StringVar(&c.certFile, "cert", envOr("VAULTFS_CERT", "/etc/vaultfs/certs/master.crt"), "TLS certificate")
	flag.StringVar(&c.keyFile, "key", envOr("VAULTFS_KEY", "/etc/vaultfs/certs/master.key"), "TLS private key")
	flag.StringVar(&c.caFile, "ca", envOr("VAULTFS_CA", "/etc/vaultfs/certs/ca.crt"), "cluster CA certificate")
	flag.Parse()

	c.raftPeers = splitList(raftPeers)
	c.chunkNodes = parseChunkNodes(chunkServers)
	return c
}

func main() {
	if err := run(parseConfig()); err != nil {
		slog.Error("master failed", "err", err)
		os.Exit(1)
	}
}

func run(c config) error {
	sec := security.Config{CertFile: c.certFile, KeyFile: c.keyFile, CAFile: c.caFile}
	serverTLS, err := sec.ServerTLSConfig()
	if err != nil {
		return err
	}
	clientTLS, err := sec.ClientTLSConfig()
	if err != nil {
		return err
	}

	store, err := metadata.Open(c.dataDir)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	ns := metadata.NewNamespace(store)
	leases := metadata.NewLeaseManager(metadata.DefaultLeaseTTL)

	mx := metrics.New()

	commitCh := make(chan raft.Entry, 256)
	raftCfg := raft.DefaultConfig(c.nodeID, c.raftPeers, commitCh)
	raftCfg.OnElection = mx.RecordElection
	node := raft.New(raftCfg)

	transport, err := raft.NewGRPCTransport(c.raftListen,
		raft.WithTLS(credentials.NewTLS(serverTLS), credentials.NewTLS(clientTLS)))
	if err != nil {
		return err
	}
	defer func() { _ = transport.Close() }()
	node.Start(transport)
	defer node.Stop()

	srv := master.New(ns, leases, node, c.chunkNodes, c.replication, master.WithMetrics(mx))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx, commitCh)
	go func() {
		if err := mx.Serve(ctx, c.metricsAddr); err != nil {
			slog.Error("metrics server stopped", "err", err)
		}
	}()

	gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	vaultfsv1.RegisterMasterServiceServer(gs, srv)
	vaultfsv1.RegisterAdminServiceServer(gs, srv)

	lis, err := net.Listen("tcp", c.listen)
	if err != nil {
		return err
	}

	slog.Info("master listening",
		"node", c.nodeID, "addr", c.listen, "raft", c.raftListen,
		"peers", len(c.raftPeers), "chunkservers", len(c.chunkNodes))
	return serve(gs, lis)
}

func serve(gs *grpc.Server, lis net.Listener) error {
	errCh := make(chan error, 1)
	go func() { errCh <- gs.Serve(lis) }()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-stop:
		slog.Info("shutting down master")
		gs.GracefulStop()
		return nil
	}
}
