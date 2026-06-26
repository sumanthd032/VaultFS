// Package cmd implements the vaultfs CLI commands on top of the public Go SDK.
package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/sumanthd032/vaultfs/internal/security"
	"github.com/sumanthd032/vaultfs/pkg/client"
)

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

	root.PersistentFlags().StringSliceVar(&opts.masters, "masters", []string{"localhost:9000"},
		"comma-separated master addresses")
	root.PersistentFlags().DurationVar(&opts.timeout, "timeout", 30*time.Second,
		"per-command timeout")
	root.PersistentFlags().StringVar(&opts.certFile, "cert", "", "client TLS certificate (enables mTLS)")
	root.PersistentFlags().StringVar(&opts.keyFile, "key", "", "client TLS private key")
	root.PersistentFlags().StringVar(&opts.caFile, "ca", "", "cluster CA certificate")
	root.PersistentFlags().StringVar(&opts.serverName, "server-name", "", "override the expected server name on master certificates")

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
