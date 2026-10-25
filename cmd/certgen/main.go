package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

type certificateAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func main() {
	outDir := flag.String("out", filepath.Join("config", "tls"), "output directory")
	force := flag.Bool("force", false, "replace existing certificate files")
	flag.Parse()

	if err := generate(*outDir, *force); err != nil {
		fmt.Fprintln(os.Stderr, "certificate generation failed:", err)
		os.Exit(1)
	}
	fmt.Println("development certificates written to", *outDir)
}

func generate(outDir string, force bool) error {
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		return err
	}
	if !force {
		for _, name := range []string{"ca.crt", "ca.key", "leader.crt", "leader.key", "member.crt", "member.key"} {
			path := filepath.Join(outDir, name)
			if _, err := os.Lstat(path); err == nil {
				return fmt.Errorf("%s already exists; use -force to replace it", path)
			} else if !os.IsNotExist(err) {
				return err
			}
		}
	}

	now := time.Now().Add(-5 * time.Minute)
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	caSerial, err := randomSerial()
	if err != nil {
		return err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "tolerex-ca", Organization: []string{"Tolerex"}},
		NotBefore:             now,
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	ca := certificateAuthority{cert: caTemplate, key: caKey}

	if err := writePair(outDir, "ca", caDER, caKey, force); err != nil {
		return err
	}

	roles := []struct {
		name string
		eku  []x509.ExtKeyUsage
	}{
		{name: "leader", eku: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}},
		{name: "member", eku: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}},
	}
	for _, role := range roles {
		if err := writeLeaf(outDir, role.name, role.eku, ca, now, force); err != nil {
			return err
		}
	}
	return nil
}

func writeLeaf(outDir, name string, eku []x509.ExtKeyUsage, ca certificateAuthority, now time.Time, force bool) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := randomSerial()
	if err != nil {
		return err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name, Organization: []string{"Tolerex"}},
		NotBefore:    now,
		NotAfter:     now.AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  eku,
		DNSNames:     []string{name, "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return err
	}
	return writePair(outDir, name, der, key, force)
}

func writePair(outDir, name string, certDER []byte, key *ecdsa.PrivateKey, force bool) error {
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	files := []struct {
		path string
		mode os.FileMode
		data []byte
	}{
		{path: filepath.Join(outDir, name+".crt"), mode: 0o644, data: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})},
		{path: filepath.Join(outDir, name+".key"), mode: 0o600, data: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})},
	}
	for _, file := range files {
		flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		if !force {
			flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
		}
		f, err := os.OpenFile(file.path, flags, file.mode)
		if err != nil {
			return err
		}
		if _, err := f.Write(file.data); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}
