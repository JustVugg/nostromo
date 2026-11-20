package main

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestCertificates(t *testing.T) {
	ca, err := newCA("test")
	if err != nil {
		t.Fatal(err)
	}
	other, _ := newCA("other")
	leaf, err := ca.sign("server", nil, []string{"a.nostromo.local"}, nil, usageServer)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	if _, err := leaf.cert.Verify(x509.VerifyOptions{Roots: pool, DNSName: "a.nostromo.local"}); err != nil {
		t.Fatalf("certificate must be valid for its own authority: %v", err)
	}
	if _, err := leaf.cert.Verify(x509.VerifyOptions{Roots: pool, DNSName: "b.nostromo.local"}); err == nil {
		t.Fatal("certificate must not be valid for another name")
	}
	pool2 := x509.NewCertPool()
	pool2.AddCert(other.cert)
	if _, err := leaf.cert.Verify(x509.VerifyOptions{Roots: pool2, DNSName: "a.nostromo.local"}); err == nil {
		t.Fatal("certificate must not be valid for another cluster's authority")
	}

	dir := t.TempDir()
	if !needsRenewal(dir, "x") {
		t.Fatal("a missing certificate needs renewal")
	}
	if err := leaf.save(dir, "x"); err != nil {
		t.Fatal(err)
	}
	if needsRenewal(dir, "x") {
		t.Fatal("a fresh certificate does not need renewal")
	}
	if c, err := loadKeyPair(dir, "x"); err != nil || c.cert.Subject.CommonName != "server" {
		t.Fatalf("reload from disk: %v", err)
	}
}

// The proxy must read the cluster name without breaking the encrypted
// connection, which is established directly between client and real server.
func TestSharedPort(t *testing.T) {
	ca, _ := newCA("test")
	leaf, _ := ca.sign("server", nil, []string{"alpha.nostromo.local"}, nil, usageServer)
	cert, err := tls.X509KeyPair(leaf.certPEM, leaf.keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go http.Serve(backend, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "hello from alpha") }))

	front, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer front.Close()
	seen := make(chan string, 1)
	go func() {
		c, err := front.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		name, consumed, err := readSNI(c)
		if err != nil {
			seen <- "error: " + err.Error()
			return
		}
		seen <- name
		b, err := net.Dial("tcp", backend.Addr().String())
		if err != nil {
			return
		}
		defer b.Close()
		b.Write(consumed)
		pipe(c, b)
	}()

	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	cl := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: pool, ServerName: "alpha.nostromo.local"},
		DisableKeepAlives: true,
	}}
	r, err := cl.Get("https://" + front.Addr().String() + "/")
	if err != nil {
		t.Fatalf("request through the proxy: %v", err)
	}
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if string(b) != "hello from alpha" {
		t.Fatalf("unexpected answer: %q", b)
	}
	if n := <-seen; n != "alpha.nostromo.local" {
		t.Fatalf("name read: %q", n)
	}
}

func TestNoServerName(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	go func() {
		c, err := net.Dial("tcp", l.Addr().String())
		if err == nil {
			c.Write([]byte("GET / HTTP/1.1\r\n\r\n")) // not TLS
			time.Sleep(200 * time.Millisecond)
			c.Close()
		}
	}()
	c, _ := l.Accept()
	defer c.Close()
	if _, _, err := readSNI(c); err == nil {
		t.Fatal("a connection without a cluster name must be rejected")
	}
}

// Periodic writes must not count as work; real ones must.
func TestWhichWritesCount(t *testing.T) {
	s := &server{tenants: map[string]*tenant{}}
	a := &tenant{Name: "alpha", srv: s, nodes: map[string]*node{}}
	s.tenants["alpha"] = a
	counts := func(ev etcdEvent) bool {
		a.lastWrite.Store(0)
		s.onEvent(ev)
		return a.lastWrite.Load() != 0
	}
	no := []string{
		"/nostromo/alpha/events/default/web.123",
		"/nostromo/alpha/masterleases/10.0.0.1",
		"/nostromo/alpha/peerserverleases/apiserver-x",
		"/nostromo/alpha/leases/kube-system/apiserver-x",
		"/nostromo/alpha/leases/kube-node-lease/node1",
		"/nostromo/beta/pods/default/x", // cluster that does not exist
		"/other/alpha/pods/default/x",
	}
	for _, k := range no {
		if counts(etcdEvent{key: k}) {
			t.Errorf("%s must not count as work", k)
		}
	}
	yes := []string{"/nostromo/alpha/pods/default/web-1", "/nostromo/alpha/deployments/default/web", "/nostromo/alpha/secrets/kube-system/x"}
	for _, k := range yes {
		if !counts(etcdEvent{key: k}) {
			t.Errorf("%s must count as work", k)
		}
	}

	// node status: creation counts, "I am alive" (same size) does not,
	// a real change (different size) does
	k := "/nostromo/alpha/minions/node1"
	if !counts(etcdEvent{key: k, created: true, valueLen: 900}) {
		t.Error("a node registering must count")
	}
	if counts(etcdEvent{key: k, valueLen: 900}) {
		t.Error("an unchanged node status must not count")
	}
	if !counts(etcdEvent{key: k, valueLen: 931}) {
		t.Error("a node status change must count")
	}
	if !counts(etcdEvent{key: k, deleted: true}) {
		t.Error("a node deletion must count")
	}
}

func TestSilentNode(t *testing.T) {
	a := &tenant{Name: "alpha", nodes: map[string]*node{}}
	a.heartbeat("node1", false)
	if a.silentNode(time.Hour) != "" {
		t.Fatal("a node that just showed a sign of life is not silent")
	}
	a.nodes["node1"].heartbeat = time.Now().Add(-2 * time.Minute)
	if a.silentNode(time.Minute) != "node1" {
		t.Fatal("a node silent for 2 minutes must be reported")
	}
	if a.silentNode(time.Minute) != "" {
		t.Fatal("the same node must not be reported twice")
	}
	a.heartbeat("node1", false) // comes back
	a.nodes["node1"].heartbeat = time.Now().Add(-2 * time.Minute)
	if a.silentNode(time.Minute) != "node1" {
		t.Fatal("if it comes back and goes silent again it must be reported again")
	}
	a.heartbeat("node1", true)
	if a.nodeCount() != 0 {
		t.Fatal("a deleted node must be forgotten")
	}
}

// After a node goes silent the engines must stay up for a while (they are the
// ones that move its pods), but not forever.
func TestNodeInTrouble(t *testing.T) {
	a := &tenant{Name: "alpha", nodes: map[string]*node{}}
	a.heartbeat("node1", false)
	if a.nodeInTrouble(time.Minute, 8*time.Minute) {
		t.Fatal("a healthy node is not trouble")
	}
	a.nodes["node1"].heartbeat = time.Now().Add(-3 * time.Minute)
	if !a.nodeInTrouble(time.Minute, 8*time.Minute) {
		t.Fatal("a node silent for 3 minutes keeps the engines up")
	}
	a.nodes["node1"].heartbeat = time.Now().Add(-time.Hour)
	if a.nodeInTrouble(time.Minute, 8*time.Minute) {
		t.Fatal("a node dead for an hour no longer keeps the engines up")
	}
}

// The watch must cover both the clusters' data and nostromo's own records.
func TestKeyLayout(t *testing.T) {
	if !(metaPrefix < rootPrefix && tenantsPrefix < prefixEnd(rootPrefix)) {
		t.Fatal("nostromo's records must sort right before the clusters' data")
	}
}

func TestNamesAndPrefixes(t *testing.T) {
	for _, n := range []string{"alpha", "a", "customer-42", "0x"} {
		if !validName.MatchString(n) {
			t.Errorf("%q should be valid", n)
		}
	}
	for _, n := range []string{"", "Alpha", "-a", "a-", "a/b", "a.b", "../x", "a b"} {
		if validName.MatchString(n) {
			t.Errorf("%q should not be valid", n)
		}
	}
	if prefixEnd("/nostromo/alpha/") != "/nostromo/alpha0" {
		t.Errorf("prefix end: %q", prefixEnd("/nostromo/alpha/"))
	}
}
