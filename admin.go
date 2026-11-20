package main

import (
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// The management API creates and deletes clusters and hands out their
// administrator credentials, so it is closed twice: it speaks only TLS, with
// a certificate from an authority of its own, and every request must carry a
// secret token. Both live in the data directory (admin-ca.crt, admin.token):
// whoever can read them is an operator. To manage nostromo from another
// machine, copy those two files there.

const (
	adminTokenFile = "admin.token"
	adminCAName    = "admin-ca"
)

// adminToken returns the token in dir, creating it on first use.
func adminToken(dir string) (string, error) {
	p := filepath.Join(dir, adminTokenFile)
	if b, err := os.ReadFile(p); err == nil && len(strings.TrimSpace(string(b))) >= 32 {
		return strings.TrimSpace(string(b)), nil
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	t := hex.EncodeToString(raw)
	return t, writeAtomic(p, []byte(t+"\n"))
}

// adminTLS returns the API's certificate, valid for the address it listens on
// and for the service's public address.
func adminTLS(dir string, hosts ...string) (*tls.Config, error) {
	ca, err := loadOrCreateCA(dir, adminCAName, "nostromo-admin")
	if err != nil {
		return nil, err
	}
	dns := []string{"localhost"}
	ips := []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			ips = append(ips, ip)
		} else if h != "" {
			dns = append(dns, h)
		}
	}
	var mu sync.Mutex
	var pair *tls.Certificate
	var expires time.Time
	get := func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		mu.Lock()
		defer mu.Unlock()
		if pair != nil && time.Until(expires) > renewBefore {
			return pair, nil
		}
		c, err := ca.sign("nostromo-admin", nil, dns, ips, usageServer)
		if err != nil {
			return nil, err
		}
		p, err := tls.X509KeyPair(c.certPEM, c.keyPEM)
		if err != nil {
			return nil, err
		}
		pair, expires = &p, c.cert.NotAfter
		return pair, nil
	}
	if _, err := get(nil); err != nil {
		return nil, err
	}
	return &tls.Config{GetCertificate: get, MinVersion: tls.VersionTLS12}, nil
}

// withToken lets through only the requests that carry the token.
func withToken(token string, next http.Handler) http.Handler {
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"missing or wrong token"}` + "\n"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// adminClient is what the commands use to talk to a running nostromo: it
// trusts only the authority found in dir and presents the token found there.
func adminClient(dir string) (*http.Client, string, error) {
	tok, err := os.ReadFile(filepath.Join(dir, adminTokenFile))
	if err != nil {
		return nil, "", errors.New("no credentials in " + dir + " (use -dir or NOSTROMO_DIR to point at nostromo's data directory)")
	}
	caPEM, err := os.ReadFile(filepath.Join(dir, adminCAName+".crt"))
	if err != nil {
		return nil, "", err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, "", errors.New("invalid " + adminCAName + ".crt in " + dir)
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}},
		strings.TrimSpace(string(tok)), nil
}
