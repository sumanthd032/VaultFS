package cmd

import (
	"context"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/sumanthd032/vaultfs/pkg/client"
)

func newLsCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "ls <remote-dir>",
		Short: "List the contents of a directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := args[0]
			return opts.withClient(func(ctx context.Context, c *client.Client) error {
				entries, err := c.ListDir(ctx, dir)
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				for _, e := range entries {
					kind := "f"
					if e.GetIsDir() {
						kind = "d"
					}
					_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\n", kind, formatSize(e.GetSize()), e.GetPath())
				}
				return tw.Flush()
			})
		},
	}
}
