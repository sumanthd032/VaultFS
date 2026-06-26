package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/sumanthd032/vaultfs/pkg/client"
)

func newPutCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "put <local-path> <remote-path>",
		Short: "Upload a local file to VaultFS",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			local, remote := args[0], args[1]
			return opts.withClient(func(ctx context.Context, c *client.Client) error {
				if err := c.Put(ctx, local, remote); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "uploaded %s -> %s\n", local, remote)
				return nil
			})
		},
	}
}
