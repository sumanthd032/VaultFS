package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/sumanthd032/vaultfs/pkg/client"
)

func newRmCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "rm <remote-path>",
		Short: "Remove a file from VaultFS",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			remote := args[0]
			return opts.withClient(func(ctx context.Context, c *client.Client) error {
				if err := c.Delete(ctx, remote); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "removed %s\n", remote)
				return nil
			})
		},
	}
}
