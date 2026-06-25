// Package main is the entry point for the VaultFS chunk server.
//
// Chunk servers store the actual file data as SHA-256-addressed chunks on local
// disk. Each chunk is replicated across three servers for fault tolerance.
// Chunk servers report stored chunks to the master via periodic heartbeats.
// Implemented fully in Step 3.
package main

import "log/slog"

func main() {
	slog.Info("vaultfs-chunkserver starting")
}
