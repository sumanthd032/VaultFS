package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/sumanthd032/vaultfs/pkg/client"
)

func newGetCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "get <remote-path> <local-path>",
		Short: "Download a file from VaultFS",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			remote, local := args[0], args[1]
			return opts.withClient(func(ctx context.Context, c *client.Client) error {
				if err := c.Get(ctx, remote, local); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "downloaded %s -> %s\n", remote, local)
				return nil
			})
		},
	}
}
