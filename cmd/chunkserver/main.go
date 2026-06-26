// Package main is the entry point for a VaultFS chunk server.
//
// It loads its mTLS identity, opens the local chunk store, and serves the
// ChunkService over gRPC. Configuration comes from flags, each with an
// environment-variable fallback so the same binary runs under docker-compose.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/sumanthd032/vaultfs/internal/chunk"
	"github.com/sumanthd032/vaultfs/internal/chunkserver"
	"github.com/sumanthd032/vaultfs/internal/metrics"
	"github.com/sumanthd032/vaultfs/internal/security"
	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

type config struct {
	nodeID      string
	listen      string
	dataDir     string
	metricsAddr string
	certFile    string
	keyFile     string
	caFile      string
}

// envOr returns the value of env, or def when unset.
func envOr(env, def string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return def
}

func parseConfig() config {
	var c config
	flag.StringVar(&c.nodeID, "node-id", envOr("VAULTFS_NODE_ID", "chunkserver-0"), "stable node identifier")
	flag.StringVar(&c.listen, "listen", envOr("VAULTFS_LISTEN", ":9100"), "gRPC listen address")
	flag.StringVar(&c.dataDir, "data-dir", envOr("VAULTFS_DATA_DIR", "/var/lib/vaultfs/chunks"), "chunk storage directory")
	flag.StringVar(&c.metricsAddr, "metrics-addr", envOr("VAULTFS_METRICS_ADDR", ":9101"), "Prometheus metrics HTTP address")
	flag.StringVar(&c.certFile, "cert", envOr("VAULTFS_CERT", "/etc/vaultfs/certs/chunkserver.crt"), "TLS certificate")
	flag.StringVar(&c.keyFile, "key", envOr("VAULTFS_KEY", "/etc/vaultfs/certs/chunkserver.key"), "TLS private key")
	flag.StringVar(&c.caFile, "ca", envOr("VAULTFS_CA", "/etc/vaultfs/certs/ca.crt"), "cluster CA certificate")
	flag.Parse()
	return c
}

func main() {
	if err := run(parseConfig()); err != nil {
		slog.Error("chunkserver failed", "err", err)
		os.Exit(1)
	}
}

func run(c config) error {
	sec := security.Config{CertFile: c.certFile, KeyFile: c.keyFile, CAFile: c.caFile}
	serverTLS, err := sec.ServerTLSConfig()
	if err != nil {
		return err
	}
	// Dialing peers derives the expected server name from the target host, so
	// ServerName is left empty in sec.
	clientTLS, err := sec.ClientTLSConfig()
	if err != nil {
		return err
	}

	store, err := chunk.NewStore(c.dataDir)
	if err != nil {
		return err
	}

	dial := func(_ context.Context, addr string) (vaultfsv1.ChunkServiceClient, func(), error) {
		cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
		if err != nil {
			return nil, func() {}, err
		}
		return vaultfsv1.NewChunkServiceClient(cc), func() { _ = cc.Close() }, nil
	}

	mx := metrics.New()
	srv := chunkserver.New(c.nodeID, store, dial, chunkserver.WithMetrics(mx))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := mx.Serve(ctx, c.metricsAddr); err != nil {
			slog.Error("metrics server stopped", "err", err)
		}
	}()

	gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	vaultfsv1.RegisterChunkServiceServer(gs, srv)

	lis, err := net.Listen("tcp", c.listen)
	if err != nil {
		return err
	}

	slog.Info("chunkserver listening", "node", c.nodeID, "addr", c.listen, "data_dir", c.dataDir)
	return serve(gs, lis)
}

// serve runs the gRPC server until a termination signal arrives, then stops it
// gracefully.
func serve(gs *grpc.Server, lis net.Listener) error {
	errCh := make(chan error, 1)
	go func() { errCh <- gs.Serve(lis) }()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-stop:
		slog.Info("shutting down chunkserver")
		gs.GracefulStop()
		return nil
	}
}
