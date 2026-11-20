package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	caLifetime   = 10 * 365 * 24 * time.Hour
	certLifetime = 365 * 24 * time.Hour
	renewBefore  = 30 * 24 * time.Hour // a certificate is renewed when it has less than this left

	usageServer     = 1
	usageClient     = 2
	usageServerPeer = usageServer | usageClient
)

// keyPair is a certificate with its private key.
type keyPair struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
	keyPEM  []byte
}

func newKey() (*ecdsa.PrivateKey, []byte, error) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	der, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		return nil, nil, err
	}
	return k, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), nil
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	return n
}

func newCA(cn string) (*keyPair, error) {
	k, kPEM, err := newKey()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: cn},
		NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(caLifetime),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		IsCA:     true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	if err != nil {
		return nil, err
	}
	c, _ := x509.ParseCertificate(der)
	return &keyPair{cert: c, key: k, keyPEM: kPEM, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

// sign issues a leaf certificate signed by this CA.
func (ca *keyPair) sign(cn string, org []string, dns []string, ips []net.IP, usage int) (*keyPair, error) {
	k, kPEM, err := newKey()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: cn, Organization: org},
		NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(certLifetime),
		KeyUsage: x509.KeyUsageDigitalSignature, DNSNames: dns, IPAddresses: ips,
	}
	if usage&usageServer != 0 {
		tpl.ExtKeyUsage = append(tpl.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
	}
	if usage&usageClient != 0 {
		tpl.ExtKeyUsage = append(tpl.ExtKeyUsage, x509.ExtKeyUsageClientAuth)
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca.cert, &k.PublicKey, ca.key)
	if err != nil {
		return nil, err
	}
	c, _ := x509.ParseCertificate(der)
	return &keyPair{cert: c, key: k, keyPEM: kPEM, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

func (c *keyPair) save(dir, name string) error {
	if err := writeAtomic(filepath.Join(dir, name+".key"), c.keyPEM); err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, name+".crt"), c.certPEM)
}

func loadKeyPair(dir, name string) (*keyPair, error) {
	cPEM, err := os.ReadFile(filepath.Join(dir, name+".crt"))
	if err != nil {
		return nil, err
	}
	kPEM, err := os.ReadFile(filepath.Join(dir, name+".key"))
	if err != nil {
		return nil, err
	}
	cb, _ := pem.Decode(cPEM)
	kb, _ := pem.Decode(kPEM)
	if cb == nil || kb == nil {
		return nil, errors.New("invalid PEM: " + name)
	}
	c, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, err
	}
	k, err := x509.ParseECPrivateKey(kb.Bytes)
	if err != nil {
		return nil, err
	}
	return &keyPair{cert: c, key: k, certPEM: cPEM, keyPEM: kPEM}, nil
}

// loadOrCreateCA loads the CA from disk, or creates it.
func loadOrCreateCA(dir, name, cn string) (*keyPair, error) {
	if c, err := loadKeyPair(dir, name); err == nil {
		return c, nil
	}
	c, err := newCA(cn)
	if err != nil {
		return nil, err
	}
	return c, c.save(dir, name)
}

// needsRenewal reports whether the certificate is missing, unreadable or expiring soon.
func needsRenewal(dir, name string) bool {
	c, err := loadKeyPair(dir, name)
	return err != nil || time.Until(c.cert.NotAfter) < renewBefore
}

// writeAtomic writes a file without ever leaving it half-written.
func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
