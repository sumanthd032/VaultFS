package security

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// expiryWarningWindow is how far before expiry a certificate triggers a warning.
const expiryWarningWindow = 30 * 24 * time.Hour

// ErrCertExpired indicates a certificate is past its NotAfter time.
var ErrCertExpired = fmt.Errorf("security: certificate expired")

// CertInfo summarises the validity window of a parsed certificate.
type CertInfo struct {
	Subject   string
	NotBefore time.Time
	NotAfter  time.Time
}

// loadLeafCertificate reads and parses the first certificate in certFile.
func loadLeafCertificate(certFile string) (*x509.Certificate, error) {
	raw, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("security: read certificate %s: %w", certFile, err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("security: no PEM block in %s", certFile)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("security: parse certificate %s: %w", certFile, err)
	}
	return cert, nil
}

// InspectCert parses certFile and returns its validity window.
func InspectCert(certFile string) (CertInfo, error) {
	cert, err := loadLeafCertificate(certFile)
	if err != nil {
		return CertInfo{}, err
	}
	return CertInfo{
		Subject:   cert.Subject.CommonName,
		NotBefore: cert.NotBefore,
		NotAfter:  cert.NotAfter,
	}, nil
}

// CheckExpiry verifies that certFile is currently valid. It returns ErrCertExpired
// if the certificate has expired, and logs a warning when expiry is within the
// warning window. now allows tests to control the reference time; pass
// time.Now() in production.
func CheckExpiry(certFile string, now time.Time) error {
	info, err := InspectCert(certFile)
	if err != nil {
		return err
	}
	if now.After(info.NotAfter) {
		return fmt.Errorf("%w: %s expired at %s", ErrCertExpired, info.Subject, info.NotAfter.Format(time.RFC3339))
	}
	if remaining := info.NotAfter.Sub(now); remaining < expiryWarningWindow {
		slog.Warn("certificate expiring soon",
			"subject", info.Subject,
			"not_after", info.NotAfter.Format(time.RFC3339),
			"remaining_hours", int(remaining.Hours()),
		)
	}
	return nil
}
