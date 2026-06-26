package cmd

import (
	"context"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/sumanthd032/vaultfs/pkg/client"
	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

func newStatusCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show cluster status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return opts.withClient(func(ctx context.Context, c *client.Client) error {
				st, err := c.Status(ctx)
				if err != nil {
					return err
				}
				out := cmd.OutOrStdout()
				_, _ = fmt.Fprintf(out, "leader:  %s\n", st.GetLeaderId())
				_, _ = fmt.Fprintf(out, "term:    %d\n", st.GetTerm())
				_, _ = fmt.Fprintf(out, "files:   %d\n", st.GetFileCount())
				_, _ = fmt.Fprintf(out, "chunks:  %d\n", st.GetChunkCount())

				nodes := st.GetNodes()
				if len(nodes) == 0 {
					return nil
				}
				_, _ = fmt.Fprintln(out, "nodes:")
				tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
				_, _ = fmt.Fprintln(tw, "  NODE\tSTATE\tCHUNKS\tLAST HEARTBEAT")
				for _, n := range nodes {
					_, _ = fmt.Fprintf(tw, "  %s\t%s\t%d\t%s\n",
						n.GetNode().GetNodeId(), nodeStateString(n.GetState()),
						n.GetChunkCount(), heartbeatAge(n.GetLastHeartbeatUnix()))
				}
				return tw.Flush()
			})
		},
	}
}

func nodeStateString(s vaultfsv1.NodeState) string {
	switch s {
	case vaultfsv1.NodeState_NODE_STATE_ALIVE:
		return "alive"
	case vaultfsv1.NodeState_NODE_STATE_DEAD:
		return "dead"
	default:
		return "unknown"
	}
}

func heartbeatAge(unix int64) string {
	if unix == 0 {
		return "never"
	}
	return time.Since(time.Unix(unix, 0)).Truncate(time.Second).String() + " ago"
}
