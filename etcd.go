package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	etcdPort     = 12379
	etcdPeerPort = 12380
	rootPrefix   = "/nostromo/" // every cluster lives under /nostromo/<name>/

	// nostromo's own records, readable by root only. They sort right before
	// rootPrefix, so a single watch covers both.
	metaPrefix    = "/nostromo-meta/"
	tenantsPrefix = metaPrefix + "tenants/" // cluster definitions (immutable)
	portsPrefix   = metaPrefix + "ports/"   // port reservations
	enginesPrefix = metaPrefix + "engines/" // which instance runs a cluster's engines
	frontPrefix   = metaPrefix + "front/"   // each cluster's front-proxy CA (immutable)
	instancesPrefix = metaPrefix + "instances/" // the running copies of nostromo
)

// etcd is the shared datastore. Every cluster has a user that can only touch
// its own slice; nostromo itself uses the root user.
type etcd struct {
	dir  string // pki directory
	host string // address the datastore listens on, when this process runs it
	proc *process
	ca   *keyPair
	http *http.Client
	url  string
	pw   string

	mu    sync.Mutex
	token string
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// prefixEnd is the key right after every key starting with p.
func prefixEnd(p string) string {
	b := []byte(p)
	b[len(b)-1]++
	return string(b)
}

// ensureEtcdPKI creates, if missing, everything needed to run and reach the
// datastore: its CA, its server certificate, nostromo's client certificate
// and the root password.
func ensureEtcdPKI(pki, host string) error {
	if err := os.MkdirAll(pki, 0o700); err != nil {
		return err
	}
	ca, err := loadOrCreateCA(pki, "etcd-ca", "nostromo-etcd-ca")
	if err != nil {
		return err
	}
	covered := false // is the current certificate good for the address we listen on?
	if c, err := loadKeyPair(pki, "etcd-server"); err == nil {
		covered = c.cert.VerifyHostname(host) == nil
	}
	// host is empty when the datastore is run by someone else: only the
	// client side is ours to keep fresh
	if host != "" && (needsRenewal(pki, "etcd-server") || !covered) {
		dns, ips := []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")}
		if ip := net.ParseIP(host); ip != nil {
			ips = append(ips, ip)
		} else {
			dns = append(dns, host)
		}
		c, err := ca.sign("etcd", nil, dns, ips, usageServerPeer)
		if err != nil {
			return err
		}
		if err := c.save(pki, "etcd-server"); err != nil {
			return err
		}
	}
	if needsRenewal(pki, "etcd-root") {
		// No common name: etcd's JSON gateway rejects client certificates that
		// carry one. nostromo's identity is the root password below.
		c, err := ca.sign("", nil, nil, nil, usageClient)
		if err != nil {
			return err
		}
		if err := c.save(pki, "etcd-root"); err != nil {
			return err
		}
	}
	pwFile := filepath.Join(pki, "etcd-root.pw")
	if _, err := os.Stat(pwFile); err != nil {
		b := make([]byte, 24)
		rand.Read(b)
		return writeAtomic(pwFile, []byte(hex.EncodeToString(b)))
	}
	return nil
}

// connectEtcd opens a client to a datastore using the credentials in pki.
func connectEtcd(pki, url string) (*etcd, error) {
	ca, err := loadKeyPair(pki, "etcd-ca")
	if err != nil {
		return nil, fmt.Errorf("datastore credentials: %w", err)
	}
	pw, err := os.ReadFile(filepath.Join(pki, "etcd-root.pw"))
	if err != nil {
		return nil, err
	}
	if _, err := tls.LoadX509KeyPair(filepath.Join(pki, "etcd-root.crt"), filepath.Join(pki, "etcd-root.key")); err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return &etcd{dir: pki, ca: ca, pw: strings.TrimSpace(string(pw)), url: url,
		http: &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool,
				GetClientCertificate: fromFiles(filepath.Join(pki, "etcd-root.crt"), filepath.Join(pki, "etcd-root.key"))},
		}}}, nil
}

// keepFresh renews, while the process runs, the datastore certificates that
// are about to expire. etcd and nostromo read the files again by themselves.
func (e *etcd) keepFresh(ctx context.Context) {
	tick := time.NewTicker(renewalCheck)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := ensureEtcdPKI(e.dir, e.host); err != nil {
				log.Printf("datastore certificates: %v", err)
			}
		}
	}
}

// portFree reports whether nothing is listening on a local port.
func portFree(host string, port int) bool {
	l, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return false
	}
	l.Close()
	return true
}

// startEtcd runs a single-member datastore and connects to it. With an empty
// listen address the datastore is private to this nostromo: it stays on the
// loopback address, on the usual port or, if that is taken (another nostromo
// on the same machine), on any free one. A listen address (host:port) is for
// a datastore that instances on other machines will reach.
func startEtcd(data, tools, listen string) (*etcd, error) {
	host, port, peerPort := "127.0.0.1", etcdPort, etcdPeerPort
	if listen != "" {
		h, p, err := net.SplitHostPort(listen)
		if err != nil {
			return nil, err
		}
		if host = h; host == "" {
			return nil, errors.New("the datastore needs an explicit address to listen on")
		}
		if port, err = strconv.Atoi(p); err != nil {
			return nil, err
		}
		peerPort = port + 1
	} else if !portFree(host, port) || !portFree(host, peerPort) {
		var err error
		if port, err = freePort(); err != nil {
			return nil, err
		}
		if peerPort, err = freePort(); err != nil {
			return nil, err
		}
	}
	etcdPeerPort := peerPort
	pki := filepath.Join(data, "pki")
	if err := ensureEtcdPKI(pki, host); err != nil {
		return nil, err
	}
	url := "https://" + net.JoinHostPort(host, strconv.Itoa(port))
	e, err := connectEtcd(pki, url)
	if err != nil {
		return nil, err
	}
	e.host = host
	e.proc, err = start(filepath.Join(data, "etcd.log"), nil, filepath.Join(tools, "etcd", "etcd"),
		"--data-dir="+filepath.Join(data, "etcd"),
		"--listen-client-urls="+url, "--advertise-client-urls="+url,
		fmt.Sprintf("--listen-peer-urls=http://127.0.0.1:%d", etcdPeerPort),
		fmt.Sprintf("--initial-advertise-peer-urls=http://127.0.0.1:%d", etcdPeerPort),
		fmt.Sprintf("--initial-cluster=default=http://127.0.0.1:%d", etcdPeerPort),
		"--client-cert-auth", "--trusted-ca-file="+filepath.Join(pki, "etcd-ca.crt"),
		"--cert-file="+filepath.Join(pki, "etcd-server.crt"), "--key-file="+filepath.Join(pki, "etcd-server.key"),
		// etcd compacts history itself: single clusters must not (compaction is global)
		// Four minutes, not five: a list continued with an old token must
		// fail within ten minutes, as it does on a cluster of its own, and
		// etcd compacts between one and two retentions after a write.
		"--auto-compaction-mode=periodic", "--auto-compaction-retention=4m",
		"--quota-backend-bytes=8589934592",
	)
	if err != nil {
		return nil, err
	}
	if err := e.setup(); err != nil {
		e.proc.stop()
		return nil, fmt.Errorf("etcd: %w", err)
	}
	return e, nil
}

// call sends a JSON request to etcd, refreshing the auth token if it expired.
func (e *etcd) call(path string, req, resp any) error {
	err := e.callOnce(path, req, resp)
	// A token also goes stale when users or roles change (a cluster created
	// through another instance): etcd then refuses writes made with it.
	if err != nil && (strings.Contains(err.Error(), "invalid auth token") ||
		strings.Contains(err.Error(), "revision of auth store is old")) {
		if err = e.login(); err == nil {
			err = e.callOnce(path, req, resp)
		}
	}
	return err
}

func (e *etcd) callOnce(path string, req, resp any) error {
	body, _ := json.Marshal(req)
	r, _ := http.NewRequest("POST", e.url+path, bytes.NewReader(body))
	e.mu.Lock()
	if e.token != "" {
		r.Header.Set("Authorization", e.token)
	}
	e.mu.Unlock()
	timeout := 10 * time.Second
	if path == "/v3/lease/keepalive" {
		timeout = keepAliveTimeout // a late answer is as bad as none
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := e.http.Do(r.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		return fmt.Errorf("%s: %s", path, strings.TrimSpace(string(b)))
	}
	if resp != nil {
		return json.Unmarshal(b, resp)
	}
	return nil
}

func (e *etcd) login() error {
	var r struct {
		Token string `json:"token"`
	}
	if err := e.callOnce("/v3/auth/authenticate", map[string]string{"name": "root", "password": e.pw}, &r); err != nil {
		return err
	}
	e.mu.Lock()
	e.token = r.Token
	e.mu.Unlock()
	return nil
}

func alreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already exists")
}

// setup waits for etcd to answer and, on first run, turns authentication on.
func (e *etcd) setup() error {
	const notEnabled = "authentication is not enabled"
	var err error
	for i := 0; i < 150; i++ {
		if e.proc != nil && !e.proc.alive() {
			return errors.New("exited at startup (see etcd.log)")
		}
		err = e.login()
		if err == nil {
			return nil
		}
		if strings.Contains(err.Error(), notEnabled) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !strings.Contains(err.Error(), notEnabled) {
		return err
	}
	if err := e.callOnce("/v3/auth/role/add", map[string]string{"name": "root"}, nil); err != nil && !alreadyExists(err) {
		return err
	}
	if err := e.callOnce("/v3/auth/user/add", map[string]string{"name": "root", "password": e.pw}, nil); err != nil && !alreadyExists(err) {
		return err
	}
	if err := e.callOnce("/v3/auth/user/grant", map[string]string{"user": "root", "role": "root"}, nil); err != nil {
		return err
	}
	if err := e.callOnce("/v3/auth/enable", map[string]string{}, nil); err != nil {
		return err
	}
	return e.login()
}

// addCluster creates the cluster's user: it can read and write only under
// /nostromo/<name>/ and authenticates with its own certificate.
func (e *etcd) addCluster(name string) error {
	p := rootPrefix + name + "/"
	if err := e.call("/v3/auth/role/add", map[string]string{"name": name}, nil); err != nil && !alreadyExists(err) {
		return err
	}
	if err := e.call("/v3/auth/role/grant", map[string]any{"name": name,
		"perm": map[string]string{"permType": "READWRITE", "key": b64(p), "range_end": b64(prefixEnd(p))}}, nil); err != nil {
		return err
	}
	if err := e.call("/v3/auth/user/add", map[string]any{"name": name, "options": map[string]bool{"no_password": true}}, nil); err != nil && !alreadyExists(err) {
		return err
	}
	return e.call("/v3/auth/user/grant", map[string]string{"user": name, "role": name}, nil)
}

// removeCluster deletes all the cluster's data and its user.
func (e *etcd) removeCluster(name string) error {
	p := rootPrefix + name + "/"
	if err := e.call("/v3/kv/deleterange", map[string]string{"key": b64(p), "range_end": b64(prefixEnd(p))}, nil); err != nil {
		return err
	}
	e.call("/v3/auth/user/delete", map[string]string{"name": name}, nil)
	e.call("/v3/auth/role/delete", map[string]string{"role": name}, nil)
	return nil
}

// clusterCert signs the certificate the cluster's apiserver presents to etcd
// (the common name is the etcd user).
func (e *etcd) clusterCert(name string) (*keyPair, error) {
	return e.ca.sign(name, nil, nil, nil, usageClient)
}

type kv struct {
	key   string
	value []byte
}

type rawKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func decodeKVs(raw []rawKV) []kv {
	out := make([]kv, 0, len(raw))
	for _, r := range raw {
		k, _ := base64.StdEncoding.DecodeString(r.Key)
		v, _ := base64.StdEncoding.DecodeString(r.Value)
		out = append(out, kv{string(k), v})
	}
	return out
}

// list returns every key starting with prefix.
func (e *etcd) list(prefix string) ([]kv, error) {
	var r struct {
		Kvs []rawKV `json:"kvs"`
	}
	err := e.call("/v3/kv/range", map[string]string{"key": b64(prefix), "range_end": b64(prefixEnd(prefix))}, &r)
	return decodeKVs(r.Kvs), err
}

// putIfAbsent writes key only if it does not exist. If it does, the current
// value is returned. A non-empty lease ties the key to that lease's life.
func (e *etcd) putIfAbsent(key string, value []byte, lease string) (bool, []byte, error) {
	put := map[string]string{"key": b64(key), "value": base64.StdEncoding.EncodeToString(value)}
	if lease != "" {
		put["lease"] = lease
	}
	var r struct {
		Succeeded bool `json:"succeeded"`
		Responses []struct {
			Range struct {
				Kvs []rawKV `json:"kvs"`
			} `json:"response_range"`
		} `json:"responses"`
	}
	err := e.call("/v3/kv/txn", map[string]any{
		"compare": []map[string]string{{"key": b64(key), "target": "CREATE", "result": "EQUAL", "create_revision": "0"}},
		"success": []map[string]any{{"request_put": put}},
		"failure": []map[string]any{{"request_range": map[string]string{"key": b64(key)}}},
	}, &r)
	if err != nil || r.Succeeded {
		return r.Succeeded, nil, err
	}
	if len(r.Responses) > 0 {
		if kvs := decodeKVs(r.Responses[0].Range.Kvs); len(kvs) > 0 {
			return false, kvs[0].value, nil
		}
	}
	return false, nil, nil
}

func (e *etcd) del(key string) error {
	return e.call("/v3/kv/deleterange", map[string]string{"key": b64(key)}, nil)
}

// delIfValue deletes key only if it still holds value (so nobody else's
// record is removed by mistake).
func (e *etcd) delIfValue(key string, value []byte) error {
	return e.call("/v3/kv/txn", map[string]any{
		"compare": []map[string]string{{"key": b64(key), "target": "VALUE", "result": "EQUAL", "value": base64.StdEncoding.EncodeToString(value)}},
		"success": []map[string]any{{"request_delete_range": map[string]string{"key": b64(key)}}},
	}, nil)
}

// grantLease creates a lease: keys tied to it vanish if it is not renewed.
func (e *etcd) grantLease(ttlSeconds int) (string, error) {
	var r struct {
		ID string `json:"ID"`
	}
	err := e.call("/v3/lease/grant", map[string]string{"TTL": fmt.Sprint(ttlSeconds)}, &r)
	if err == nil && r.ID == "" {
		err = errors.New("lease grant returned no id")
	}
	return r.ID, err
}

// keepAlive renews a lease and reports whether it is still alive.
// keepAlive renews a lease. gone means etcd answered that the lease no longer
// exists; otherwise a failure may just be a slow or unreachable etcd.
func (e *etcd) keepAlive(id string) (ok, gone bool) {
	var r struct {
		Result struct {
			TTL string `json:"TTL"`
		} `json:"result"`
	}
	if err := e.call("/v3/lease/keepalive", map[string]string{"ID": id}, &r); err != nil {
		return false, false
	}
	ok = r.Result.TTL != "" && r.Result.TTL != "0"
	return ok, !ok
}

type etcdEvent struct {
	key      string
	deleted  bool
	created  bool
	valueLen int
	value    []byte // only for nostromo's own records
}

// watch follows every write of every cluster, and nostromo's own records, in a single stream. If the
// stream drops it is reopened and lost() is called, since writes may have
// been missed in the meantime.
func (e *etcd) watch(ctx context.Context, on func(etcdEvent), lost func()) {
	for ctx.Err() == nil {
		if err := e.watchOnce(ctx, on); err != nil && ctx.Err() == nil {
			log.Printf("etcd: watch interrupted (%v), reopening", err)
			e.login()
			lost()
			time.Sleep(time.Second)
		}
	}
}

func (e *etcd) watchOnce(ctx context.Context, on func(etcdEvent)) error {
	body, _ := json.Marshal(map[string]any{"create_request": map[string]string{
		"key": b64(metaPrefix), "range_end": b64(prefixEnd(rootPrefix))}})
	r, _ := http.NewRequestWithContext(ctx, "POST", e.url+"/v3/watch", bytes.NewReader(body))
	e.mu.Lock()
	r.Header.Set("Authorization", e.token)
	e.mu.Unlock()
	res, err := e.http.Do(r)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 500))
		return errors.New(strings.TrimSpace(string(b)))
	}
	dec := json.NewDecoder(res.Body)
	for {
		var m struct {
			Result struct {
				Canceled bool `json:"canceled"`
				Events   []struct {
					Type string `json:"type"`
					Kv   struct {
						Key            string `json:"key"`
						CreateRevision string `json:"create_revision"`
						ModRevision    string `json:"mod_revision"`
						Value          string `json:"value"`
					} `json:"kv"`
				} `json:"events"`
			} `json:"result"`
			Error json.RawMessage `json:"error"`
		}
		if err := dec.Decode(&m); err != nil {
			return err
		}
		if len(m.Error) > 0 {
			return errors.New(string(m.Error))
		}
		if m.Result.Canceled {
			return errors.New("watch canceled by etcd")
		}
		for _, ev := range m.Result.Events {
			k, err := base64.StdEncoding.DecodeString(ev.Kv.Key)
			if err != nil {
				continue
			}
			out := etcdEvent{
				key:      string(k),
				deleted:  ev.Type == "DELETE",
				created:  ev.Type != "DELETE" && ev.Kv.CreateRevision == ev.Kv.ModRevision,
				valueLen: base64.RawStdEncoding.DecodedLen(len(strings.TrimRight(ev.Kv.Value, "="))),
			}
			if strings.HasPrefix(out.key, metaPrefix) {
				out.value, _ = base64.StdEncoding.DecodeString(ev.Kv.Value)
			}
			on(out)
		}
	}
}
