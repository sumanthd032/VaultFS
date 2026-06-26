// Package security builds the mutual-TLS configuration used by every VaultFS
// gRPC endpoint. Both servers and clients present a certificate signed by the
// shared cluster CA and verify their peer against that CA, so every node
// authenticates every other node.
package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// ErrNoCertificate indicates that the configured cert/key pair could not be
// loaded.
var ErrNoCertificate = errors.New("security: certificate not loaded")

// Config names the PEM files that make up a node's TLS identity.
type Config struct {
	// CertFile and KeyFile are the node's own certificate and private key.
	CertFile string
	KeyFile  string
	// CAFile is the cluster CA used to verify peer certificates.
	CAFile string
	// ServerName is the expected name on the peer certificate when dialing.
	// It is ignored for server-side configs.
	ServerName string
}

// loadCertPool reads caFile and returns a pool containing its certificates.
func loadCertPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("security: read CA %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("security: no valid certificates in CA %s", caFile)
	}
	return pool, nil
}

// loadKeyPair loads the node's certificate and private key.
func (c Config) loadKeyPair() (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("security: load key pair: %w", err)
	}
	return cert, nil
}

// ServerTLSConfig builds a tls.Config for a gRPC server. It presents the node's
// certificate and requires clients to present a certificate signed by the
// cluster CA (mutual TLS).
func (c Config) ServerTLSConfig() (*tls.Config, error) {
	cert, err := c.loadKeyPair()
	if err != nil {
		return nil, err
	}
	caPool, err := loadCertPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ClientTLSConfig builds a tls.Config for a gRPC client. It presents the node's
// certificate and verifies the server against the cluster CA. ServerName, when
// set, overrides the name checked against the server certificate.
func (c Config) ClientTLSConfig() (*tls.Config, error) {
	cert, err := c.loadKeyPair()
	if err != nil {
		return nil, err
	}
	caPool, err := loadCertPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		ServerName:   c.ServerName,
		MinVersion:   tls.VersionTLS13,
	}, nil
}
