// Package main is the entry point for the VaultFS CLI.
//
// The CLI wraps the public Go SDK (pkg/client) with Cobra commands:
// put, get, ls, rm, and status. Each command connects to the master
// cluster, authenticates via mTLS, and delegates to the SDK.
// Implemented fully in Step 4.
package main

import "log/slog"

func main() {
	slog.Info("vaultfs-cli starting")
}
