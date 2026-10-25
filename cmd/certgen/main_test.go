package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateCreatesUsableCertificatePairs(t *testing.T) {
	dir := t.TempDir()
	if err := generate(dir, false); err != nil {
		t.Fatal(err)
	}

	ca := readCertificate(t, filepath.Join(dir, "ca.crt"))
	roots := x509.NewCertPool()
	roots.AddCert(ca)

	for _, name := range []string{"leader", "member"} {
		certPath := filepath.Join(dir, name+".crt")
		keyPath := filepath.Join(dir, name+".key")
		if _, err := tls.LoadX509KeyPair(certPath, keyPath); err != nil {
			t.Fatalf("%s key pair is invalid: %v", name, err)
		}
		leaf := readCertificate(t, certPath)
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: name}); err != nil {
			t.Fatalf("%s certificate verification failed: %v", name, err)
		}
	}

	if err := generate(dir, false); err == nil {
		t.Fatal("expected generation without -force to preserve existing files")
	}
	if err := generate(dir, true); err != nil {
		t.Fatalf("forced regeneration failed: %v", err)
	}
}

func readCertificate(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("no PEM certificate in %s", path)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}
