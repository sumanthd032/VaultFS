package security

import (
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateDevPKIWritesFiles(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateDevPKI(dir); err != nil {
		t.Fatalf("GenerateDevPKI: %v", err)
	}

	want := []string{
		"ca.crt", "ca.key",
		"master.crt", "master.key",
		"chunkserver.crt", "chunkserver.key",
		"client.crt", "client.key",
	}
	for _, name := range want {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("expected %s: %v", name, err)
		}
		if filepath.Ext(name) == ".key" && info.Mode().Perm() != 0o600 {
			t.Errorf("%s mode = %o, want 600", name, info.Mode().Perm())
		}
	}
}

func TestGenerateDevPKILeavesVerifyAgainstCA(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateDevPKI(dir); err != nil {
		t.Fatalf("GenerateDevPKI: %v", err)
	}

	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("CA not added to pool")
	}

	tests := []struct {
		name    string
		cert    string
		dnsName string
	}{
		{name: "master serves master-0", cert: "master.crt", dnsName: "master-0"},
		{name: "chunkserver serves chunkserver-1", cert: "chunkserver.crt", dnsName: "chunkserver-1"},
		{name: "client serves localhost", cert: "client.crt", dnsName: "localhost"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leaf, err := loadLeafCertificate(filepath.Join(dir, tt.cert))
			if err != nil {
				t.Fatal(err)
			}
			// Both usages are present so any node can dial or serve.
			_, err = leaf.Verify(x509.VerifyOptions{
				Roots:     roots,
				DNSName:   tt.dnsName,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			})
			if err != nil {
				t.Fatalf("verify %s for %s: %v", tt.cert, tt.dnsName, err)
			}
		})
	}
}

// TestGenerateDevPKIBuildsTLSConfigs proves the generated material loads through
// the same Config the daemons use for mutual TLS.
func TestGenerateDevPKIBuildsTLSConfigs(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateDevPKI(dir); err != nil {
		t.Fatalf("GenerateDevPKI: %v", err)
	}

	serverCfg := Config{
		CertFile: filepath.Join(dir, "master.crt"),
		KeyFile:  filepath.Join(dir, "master.key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	}
	if _, err := serverCfg.ServerTLSConfig(); err != nil {
		t.Fatalf("ServerTLSConfig: %v", err)
	}

	clientCfg := Config{
		CertFile:   filepath.Join(dir, "client.crt"),
		KeyFile:    filepath.Join(dir, "client.key"),
		CAFile:     filepath.Join(dir, "ca.crt"),
		ServerName: "localhost",
	}
	if _, err := clientCfg.ClientTLSConfig(); err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
}

func TestGenerateDevPKICertsAreCurrentlyValid(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateDevPKI(dir); err != nil {
		t.Fatalf("GenerateDevPKI: %v", err)
	}
	if err := CheckExpiry(filepath.Join(dir, "master.crt"), time.Now()); err != nil {
		t.Fatalf("master cert should be valid: %v", err)
	}
}
