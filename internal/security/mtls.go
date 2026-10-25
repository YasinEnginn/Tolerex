package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

func loadCertificatePool(path string) (*x509.CertPool, error) {
	pemData, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemData) {
		return nil, fmt.Errorf("CA file %q does not contain a valid certificate", path)
	}
	return pool, nil
}

// NewMTLSServerCreds requires a client certificate signed by caFile.
func NewMTLSServerCreds(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	clientCAs, err := loadCertificatePool(caFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

// NewMTLSClientCreds presents a client certificate and verifies serverName.
func NewMTLSClientCreds(certFile, keyFile, caFile, serverName string) (credentials.TransportCredentials, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	roots, err := loadCertificatePool(caFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate},
		RootCAs:      roots,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
	}), nil
}
