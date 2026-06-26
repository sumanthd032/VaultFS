// Package main is the entry point for the VaultFS CLI.
//
// It wires the Cobra command tree (defined in the cmd package) which delegates
// to the public Go SDK (pkg/client). Command logic lives in cmd/; this file
// only parses arguments and reports the exit status.
package main

import (
	"fmt"
	"os"

	"github.com/sumanthd032/vaultfs/cmd/vaultfs/cmd"
)

func main() {
	if err := cmd.NewRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "vaultfs:", err)
		os.Exit(1)
	}
}
