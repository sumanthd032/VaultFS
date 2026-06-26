package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Validity windows for the development PKI, mirroring the previous openssl
// script: a long-lived CA and leaf certificates just under the 825-day limit
// browsers and tooling enforce.
const (
	devCAValidity   = 3650 * 24 * time.Hour
	devLeafValidity = 825 * 24 * time.Hour
)

// devCertSpec describes a leaf certificate to issue for the development PKI.
type devCertSpec struct {
	name     string
	dnsNames []string
	ipAddrs  []net.IP
}

// GenerateDevPKI writes a self-signed cluster CA and the master, chunkserver,
// and client certificates into dir, creating it if necessary. Every leaf
// certificate carries both serverAuth and clientAuth extended key usages so any
// node can authenticate to any other under mutual TLS, with SANs covering the
// docker-compose service hostnames and localhost.
//
// It is a pure-Go replacement for the former openssl shell script, so the only
// requirement to produce dev certificates is the Go toolchain. It is intended
// for local development; production deployments should use cert-manager or
// another managed PKI.
func GenerateDevPKI(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("security: create cert dir %s: %w", dir, err)
	}

	caCert, caKey, err := generateCA()
	if err != nil {
		return err
	}
	if err := writeCertPEM(filepath.Join(dir, "ca.crt"), caCert.Raw); err != nil {
		return err
	}
	if err := writeKeyPEM(filepath.Join(dir, "ca.key"), caKey); err != nil {
		return err
	}

	localhost := net.IPv4(127, 0, 0, 1)
	specs := []devCertSpec{
		{name: "master", dnsNames: []string{"localhost", "master-0", "master-1", "master-2"}, ipAddrs: []net.IP{localhost}},
		{name: "chunkserver", dnsNames: []string{"localhost", "chunkserver-0", "chunkserver-1", "chunkserver-2"}, ipAddrs: []net.IP{localhost}},
		{name: "client", dnsNames: []string{"localhost"}, ipAddrs: []net.IP{localhost}},
	}
	for _, spec := range specs {
		if err := issueLeaf(dir, spec, caCert, caKey); err != nil {
			return err
		}
	}
	return nil
}

// generateCA creates a self-signed P-256 certificate authority.
func generateCA() (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("security: generate CA key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "VaultFS-CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(devCAValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("security: create CA certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, fmt.Errorf("security: parse CA certificate: %w", err)
	}
	return cert, key, nil
}

// issueLeaf creates a P-256 leaf certificate for spec, signs it with the CA, and
// writes the cert and key PEM files into dir.
func issueLeaf(dir string, spec devCertSpec, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("security: generate %s key: %w", spec.name, err)
	}
	serial, err := randomSerial()
	if err != nil {
		return err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: spec.name},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(devLeafValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     spec.dnsNames,
		IPAddresses:  spec.ipAddrs,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("security: create %s certificate: %w", spec.name, err)
	}
	if err := writeCertPEM(filepath.Join(dir, spec.name+".crt"), der); err != nil {
		return err
	}
	return writeKeyPEM(filepath.Join(dir, spec.name+".key"), key)
}

// randomSerial returns a random 128-bit certificate serial number.
func randomSerial() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("security: generate serial: %w", err)
	}
	return serial, nil
}

// writeCertPEM writes a DER certificate to path as a PEM file. The dev PKI is
// owned and read by a single user (or root inside the compose containers), so
// the certificates are written owner-only alongside their keys.
func writeCertPEM(path string, der []byte) error {
	buf := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		return fmt.Errorf("security: write %s: %w", path, err)
	}
	return nil
}

// writeKeyPEM writes an EC private key to path as a PKCS#8 PEM file with
// owner-only permissions.
func writeKeyPEM(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return fmt.Errorf("security: marshal private key: %w", err)
	}
	buf := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		return fmt.Errorf("security: write %s: %w", path, err)
	}
	return nil
}
