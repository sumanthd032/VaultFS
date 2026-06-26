// Package cmd implements the vaultfs CLI commands on top of the public Go SDK.
package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/sumanthd032/vaultfs/internal/security"
	"github.com/sumanthd032/vaultfs/pkg/client"
)

// envOr returns the value of key, or def when the variable is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// mastersDefault reads the default master list from VAULTFS_MASTERS, falling
// back to a local single-node address.
func mastersDefault() []string {
	if v := os.Getenv("VAULTFS_MASTERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9000"}
}

// options holds the global flags shared by all subcommands.
type options struct {
	masters    []string
	timeout    time.Duration
	certFile   string
	keyFile    string
	caFile     string
	serverName string
}

// NewRootCommand builds the root vaultfs command with all subcommands attached.
func NewRootCommand() *cobra.Command {
	opts := &options{}

	root := &cobra.Command{
		Use:           "vaultfs",
		Short:         "VaultFS distributed filesystem client",
		Long:          "vaultfs is the command-line client for the VaultFS distributed filesystem.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// Flags default to their VAULTFS_* environment variables when set, so the
	// same binary is convenient on a developer's host and inside a container.
	// An explicit flag always overrides the environment.
	root.PersistentFlags().StringSliceVar(&opts.masters, "masters", mastersDefault(),
		"comma-separated master addresses (env VAULTFS_MASTERS)")
	root.PersistentFlags().DurationVar(&opts.timeout, "timeout", 30*time.Second,
		"per-command timeout")
	root.PersistentFlags().StringVar(&opts.certFile, "cert", envOr("VAULTFS_CERT", ""), "client TLS certificate (enables mTLS) (env VAULTFS_CERT)")
	root.PersistentFlags().StringVar(&opts.keyFile, "key", envOr("VAULTFS_KEY", ""), "client TLS private key (env VAULTFS_KEY)")
	root.PersistentFlags().StringVar(&opts.caFile, "ca", envOr("VAULTFS_CA", ""), "cluster CA certificate (env VAULTFS_CA)")
	root.PersistentFlags().StringVar(&opts.serverName, "server-name", envOr("VAULTFS_SERVER_NAME", ""), "override the expected server name on master certificates (env VAULTFS_SERVER_NAME)")

	root.AddCommand(
		newPutCommand(opts),
		newGetCommand(opts),
		newLsCommand(opts),
		newRmCommand(opts),
		newStatusCommand(opts),
	)
	return root
}

// withClient opens an SDK client and a context with the configured timeout,
// runs fn, and cleans both up. It centralises connection and context handling
// so each command stays focused on its own logic.
func (o *options) withClient(fn func(ctx context.Context, c *client.Client) error) error {
	cfg := client.Config{MasterAddrs: o.masters}

	// When a certificate is supplied, dial the cluster over mutual TLS.
	if o.certFile != "" {
		tlsCfg, err := security.Config{
			CertFile: o.certFile, KeyFile: o.keyFile, CAFile: o.caFile, ServerName: o.serverName,
		}.ClientTLSConfig()
		if err != nil {
			return err
		}
		cfg.DialOptions = []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))}
	}

	c, err := client.New(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()
	return fn(ctx, c)
}

// formatSize renders a byte count in a compact human-readable form.
func formatSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}
