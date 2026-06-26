// Command gen-certs writes a local development PKI for running VaultFS with
// mutual TLS: a self-signed cluster CA plus the master, chunkserver, and client
// certificates. It replaces the former openssl shell script, so the only
// requirement is the Go toolchain.
//
// Usage: gen-certs [output-dir]   (default: deploy/certs)
package main

import (
	"log/slog"
	"os"

	"github.com/sumanthd032/vaultfs/internal/security"
)

func main() {
	dir := "deploy/certs"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	if err := security.GenerateDevPKI(dir); err != nil {
		slog.Error("gen-certs failed", "err", err)
		os.Exit(1)
	}
	slog.Info("certificates written", "dir", dir)
}
