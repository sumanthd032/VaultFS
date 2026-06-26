package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testCerts writes a CA plus one leaf certificate (valid for the given window)
// into dir and returns a Config pointing at them. The leaf carries both server
// and client extended key usages and SANs for localhost, so it works for mutual
// TLS in either role.
func testCerts(t *testing.T, dir string, notBefore, notAfter time.Time) Config {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "VaultFS-Test-CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create ca: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}

	caPath := filepath.Join(dir, "ca.crt")
	certPath := filepath.Join(dir, "node.crt")
	keyPath := filepath.Join(dir, "node.key")

	writePEM(t, caPath, "CERTIFICATE", caDER)
	writePEM(t, certPath, "CERTIFICATE", leafDER)
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)

	return Config{CertFile: certPath, KeyFile: keyPath, CAFile: caPath, ServerName: "localhost"}
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
}

func TestServerAndClientConfigsLoad(t *testing.T) {
	dir := t.TempDir()
	cfg := testCerts(t, dir, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))

	if _, err := cfg.ServerTLSConfig(); err != nil {
		t.Errorf("ServerTLSConfig: %v", err)
	}
	if _, err := cfg.ClientTLSConfig(); err != nil {
		t.Errorf("ClientTLSConfig: %v", err)
	}
}

func TestConfigMissingFiles(t *testing.T) {
	cfg := Config{CertFile: "/nope/a.crt", KeyFile: "/nope/a.key", CAFile: "/nope/ca.crt"}
	if _, err := cfg.ServerTLSConfig(); err == nil {
		t.Error("ServerTLSConfig should fail with missing files")
	}
	if _, err := cfg.ClientTLSConfig(); err == nil {
		t.Error("ClientTLSConfig should fail with missing files")
	}
}

// TestMutualHandshake performs a real TLS 1.3 handshake between a server config
// and a client config built from CA-signed certs, proving both sides
// authenticate each other.
func TestMutualHandshake(t *testing.T) {
	dir := t.TempDir()
	cfg := testCerts(t, dir, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))

	serverCfg, err := cfg.ServerTLSConfig()
	if err != nil {
		t.Fatalf("server config: %v", err)
	}
	clientCfg, err := cfg.ClientTLSConfig()
	if err != nil {
		t.Fatalf("client config: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	errCh := make(chan error, 1)
	go func() {
		s := tls.Server(serverConn, serverCfg)
		errCh <- s.Handshake()
		_ = s.Close()
	}()

	c := tls.Client(clientConn, clientCfg)
	if err := c.Handshake(); err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	_ = c.Close()
}

// TestHandshakeRejectsUntrustedClient proves the server refuses a client whose
// certificate is signed by a different CA.
func TestHandshakeRejectsUntrustedClient(t *testing.T) {
	serverCfg, err := testCerts(t, t.TempDir(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour)).ServerTLSConfig()
	if err != nil {
		t.Fatalf("server config: %v", err)
	}
	// Client uses a completely separate CA, untrusted by the server.
	clientCfg, err := testCerts(t, t.TempDir(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour)).ClientTLSConfig()
	if err != nil {
		t.Fatalf("client config: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	go func() {
		s := tls.Server(serverConn, serverCfg)
		_ = s.Handshake()
		_ = s.Close()
	}()

	c := tls.Client(clientConn, clientCfg)
	if err := c.Handshake(); err == nil {
		t.Error("handshake should fail for a client signed by an untrusted CA")
		_ = c.Close()
	}
}

func TestCheckExpiry(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name      string
		notBefore time.Time
		notAfter  time.Time
		wantErr   bool
	}{
		{"valid", now.Add(-time.Hour), now.Add(365 * 24 * time.Hour), false},
		{"expiring soon warns but ok", now.Add(-time.Hour), now.Add(24 * time.Hour), false},
		{"expired", now.Add(-48 * time.Hour), now.Add(-time.Hour), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCerts(t, t.TempDir(), tt.notBefore, tt.notAfter)
			err := CheckExpiry(cfg.CertFile, now)
			if tt.wantErr && !errors.Is(err, ErrCertExpired) {
				t.Errorf("got %v, want ErrCertExpired", err)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestInspectCert(t *testing.T) {
	notAfter := time.Now().Add(100 * time.Hour).Truncate(time.Second)
	cfg := testCerts(t, t.TempDir(), time.Now().Add(-time.Hour), notAfter)
	info, err := InspectCert(cfg.CertFile)
	if err != nil {
		t.Fatalf("InspectCert: %v", err)
	}
	if info.Subject != "localhost" {
		t.Errorf("Subject = %q, want localhost", info.Subject)
	}
	if !info.NotAfter.Equal(notAfter) {
		t.Errorf("NotAfter = %v, want %v", info.NotAfter, notAfter)
	}
}
