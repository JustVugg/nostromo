package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// node is what nostromo remembers about a worker node of a cluster.
type node struct {
	heartbeat  time.Time // last sign of life (lease renewal)
	reported   bool      // the engines were already woken for its silence
	statusSize int       // size of the last status written by the node
}

// tenant is a hosted control plane. It has three levels:
//   - asleep:  no process at all
//   - standby: only the apiserver (answers users and nodes)
//   - full:    apiserver + "engines" (controller-manager and scheduler)
type tenant struct {
	Name string
	Port int // dedicated port: nodes and pods reach the cluster here, by address
	// AgentPort: nodes keep a tunnel open here, so the control plane can reach
	// them (logs, exec, webhooks) even when they sit on another network.
	AgentPort int

	dir      string
	srv      *server
	listener net.Listener
	agentListener net.Listener // where the nodes' tunnel agents call
	tunnelPort    int          // where the tunnel server takes them, on this machine

	mu           sync.Mutex
	api, cm, sc  *process
	tunnel       *process // Konnectivity server; lives and dies with the apiserver
	internalPort int
	kube         *http.Client
	conns        int
	lastActive   time.Time
	wakes        int
	lastWakeMs   int64
	lastRenewal  time.Time // last certificate expiry check

	enginesStartedAt time.Time
	enginesStoppedAt time.Time
	justWoke         bool      // the apiserver just started: engines are due
	owed             bool      // the engines were stopped mid-work (lease lost): resume
	warming          bool      // first boot done in the background, nobody has used the cluster yet
	holdsLock        bool      // this instance is the one running the cluster's engines
	nextLockTry      time.Time // another instance holds the engines: when to look again
	nextTimedCheck   time.Time
	engineStarts     int
	lastReason       string

	lastWrite atomic.Int64 // UnixNano of the last "real" write seen in etcd

	nodesMu sync.Mutex
	nodes   map[string]*node
}

func (t *tenant) file(n string) string { return filepath.Join(t.dir, n) }

// prepare creates (or renews) the cluster's keys and certificates. Every
// cluster has its own authority: one cluster's certificates are worthless
// for another.
func (t *tenant) prepare() error {
	ca, err := loadOrCreateCA(t.dir, "ca", "nostromo-"+t.Name)
	if err != nil {
		return err
	}
	if _, err := os.Stat(t.file("sa.key")); err != nil {
		k, kPEM, err := newKey()
		if err != nil {
			return err
		}
		pub, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
		if err != nil {
			return err
		}
		if err := writeAtomic(t.file("sa.pub"), pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub})); err != nil {
			return err
		}
		if err := writeAtomic(t.file("sa.key"), kPEM); err != nil {
			return err
		}
	}
	ips := []net.IP{net.ParseIP("127.0.0.1"), t.srv.serviceIP}
	dns := []string{"localhost", "kubernetes", "kubernetes.default", "kubernetes.default.svc",
		"kubernetes.default.svc.cluster.local", t.Name + "." + t.srv.domain}
	for _, a := range []string{t.srv.address, t.srv.nodeAddress} {
		if ip := net.ParseIP(a); ip != nil {
			ips = append(ips, ip)
		} else if a != "" {
			dns = append(dns, a)
		}
	}
	leaves := []struct {
		name, cn string
		org      []string
		usage    int
	}{
		{"apiserver", "kube-apiserver", nil, usageServer},
		{"admin", "kubernetes-admin", []string{"system:masters"}, usageClient},
		{"cm", "system:kube-controller-manager", nil, usageClient},
		{"sched", "system:kube-scheduler", nil, usageClient},
		{"kubelet-client", "kube-apiserver-kubelet-client", []string{"system:masters"}, usageClient},
		{"konnectivity", "system:konnectivity-server", nil, usageClient},
	}
	// The identity the apiserver shows to extension API servers (aggregation).
	front, err := loadOrCreateCA(t.dir, "front-ca", "nostromo-"+t.Name+"-front-proxy")
	if err != nil {
		return err
	}
	if needsRenewal(t.dir, "front-proxy-client") {
		c, err := front.sign("front-proxy-client", nil, nil, nil, usageClient)
		if err != nil {
			return err
		}
		if err := c.save(t.dir, "front-proxy-client"); err != nil {
			return err
		}
	}
	for _, l := range leaves {
		if !needsRenewal(t.dir, l.name) {
			continue
		}
		var c *keyPair
		if l.usage == usageServer {
			c, err = ca.sign(l.cn, l.org, dns, ips, l.usage)
		} else {
			c, err = ca.sign(l.cn, l.org, nil, nil, l.usage)
		}
		if err != nil {
			return err
		}
		if err := c.save(t.dir, l.name); err != nil {
			return err
		}
	}
	if needsRenewal(t.dir, "etcd") {
		c, err := t.srv.etcd.clusterCert(t.Name)
		if err != nil {
			return err
		}
		if err := c.save(t.dir, "etcd"); err != nil {
			return err
		}
	}
	return nil
}

// adminKubeconfig is the file handed to the cluster's user: self-contained,
// it goes through the shared port and names the cluster in the TLS hello.
func (t *tenant) adminKubeconfig() ([]byte, error) {
	return t.kubeconfig(net.JoinHostPort(t.srv.address, fmt.Sprint(t.srv.port)), t.Name+"."+t.srv.domain)
}

// nodeKubeconfig reaches the cluster on its dedicated port by plain address,
// the way nodes and pods do.
func (t *tenant) nodeKubeconfig() ([]byte, error) {
	return t.kubeconfig(t.nodeEndpoint(), "")
}

func (t *tenant) nodeEndpoint() string {
	return net.JoinHostPort(t.srv.nodeAddress, fmt.Sprint(t.Port))
}

func (t *tenant) kubeconfig(server, serverName string) ([]byte, error) {
	read := func(n string) (string, error) {
		b, err := os.ReadFile(t.file(n))
		return base64.StdEncoding.EncodeToString(b), err
	}
	ca, err := read("ca.crt")
	if err != nil {
		return nil, err
	}
	crt, err := read("admin.crt")
	if err != nil {
		return nil, err
	}
	key, err := read("admin.key")
	if err != nil {
		return nil, err
	}
	sni := ""
	if serverName != "" {
		sni = "\n    tls-server-name: " + serverName
	}
	return []byte(fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: %[1]s
  cluster:
    server: https://%[2]s%[3]s
    certificate-authority-data: %[4]s
users:
- name: %[1]s-admin
  user:
    client-certificate-data: %[5]s
    client-key-data: %[6]s
contexts:
- name: %[1]s
  context: {cluster: %[1]s, user: %[1]s-admin}
current-context: %[1]s
`, t.Name, server, sni, ca, crt, key)), nil
}

// internalKubeconfig is for the engines: they talk to the apiserver without
// going through the proxy, so their connections do not count as "someone is
// using the cluster".
func (t *tenant) internalKubeconfig(who string) error {
	return writeAtomic(t.file(who+".kubeconfig"), []byte(fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: c
  cluster: {server: "https://127.0.0.1:%d", tls-server-name: localhost, certificate-authority: %q}
users:
- name: u
  user: {client-certificate: %q, client-key: %q}
contexts:
- name: c
  context: {cluster: c, user: u}
current-context: c
`, t.internalPort, t.file("ca.crt"), t.file(who+".crt"), t.file(who+".key"))))
}

func (t *tenant) kubeClient() (*http.Client, error) {
	ca, err := os.ReadFile(t.file("ca.crt"))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	if _, err := tls.LoadX509KeyPair(t.file("admin.crt"), t.file("admin.key")); err != nil {
		return nil, err
	}
	return &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: pool, GetClientCertificate: fromFiles(t.file("admin.crt"), t.file("admin.key")), ServerName: "localhost"},
		DisableKeepAlives: true,
	}}, nil
}

// get sends a GET to the cluster's apiserver and decodes the JSON answer.
func (t *tenant) get(path string, out any) error {
	return t.request("GET", path, "", nil, out)
}

// apply creates or updates an object in the cluster (server-side apply).
func (t *tenant) apply(path string, obj any) error {
	body, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return t.request("PATCH", path+"?fieldManager=nostromo&force=true", "application/apply-patch+yaml", body, nil)
}

func (t *tenant) request(method, path, contentType string, body []byte, out any) error {
	req, err := http.NewRequest(method, fmt.Sprintf("https://127.0.0.1:%d%s", t.internalPort, path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	r, err := t.kube.Do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	b, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return err
	}
	if r.StatusCode >= 300 {
		return fmt.Errorf("%s %s: status %d", method, path, r.StatusCode)
	}
	switch o := out.(type) {
	case nil:
		return nil
	case *string:
		*o = string(b)
		return nil
	}
	return json.Unmarshal(b, out)
}

// publishEndpoints tells the cluster where its own API lives: the address and
// port nodes and pods must use (the "kubernetes" service). The apiserver
// cannot know it by itself, since it sits behind nostromo.
func (t *tenant) publishEndpoints() {
	ip := t.srv.nodeAddress
	port := map[string]any{"name": "https", "port": t.Port, "protocol": "TCP"}
	var err error
	for i := 0; i < 20; i++ { // the default namespace may take a moment on first boot
		err = t.apply("/apis/discovery.k8s.io/v1/namespaces/default/endpointslices/kubernetes", map[string]any{
			"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
			"metadata": map[string]any{"name": "kubernetes", "namespace": "default",
				"labels": map[string]string{"kubernetes.io/service-name": "kubernetes"}},
			"addressType": "IPv4",
			"endpoints":   []any{map[string]any{"addresses": []string{ip}, "conditions": map[string]bool{"ready": true}}},
			"ports":       []any{port},
		})
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		log.Printf("[%s] publishing the API endpoint: %v", t.Name, err)
		return
	}
	// the older Endpoints object, still read by some clients
	if err := t.apply("/api/v1/namespaces/default/endpoints/kubernetes", map[string]any{
		"apiVersion": "v1", "kind": "Endpoints",
		"metadata": map[string]any{"name": "kubernetes", "namespace": "default",
			"labels": map[string]string{"endpointslice.kubernetes.io/skip-mirror": "true"}},
		"subsets": []any{map[string]any{"addresses": []any{map[string]string{"ip": ip}}, "ports": []any{port}}},
	}); err != nil {
		log.Printf("[%s] publishing the legacy API endpoint: %v", t.Name, err)
	}
}

// wake starts the apiserver if it is down and waits until it is ready.
// Must be called with t.mu held.
func (t *tenant) wake() error {
	if t.api.alive() {
		return nil
	}
	t.stopEngines() // if the apiserver crashed, the engines point at a stale port
	begin := time.Now()
	if err := t.prepare(); err != nil {
		return err
	}
	var err error
	if t.internalPort, err = freePort(); err != nil {
		return err
	}
	if t.kube, err = t.kubeClient(); err != nil {
		return err
	}
	s := t.srv
	args := []string{
		"--etcd-servers=" + s.etcd.url,
		"--etcd-prefix=" + rootPrefix + t.Name,
		"--etcd-cafile=" + filepath.Join(s.etcd.dir, "etcd-ca.crt"),
		"--etcd-certfile=" + t.file("etcd.crt"), "--etcd-keyfile=" + t.file("etcd.key"),
		"--etcd-compaction-interval=0", // etcd does it for everyone
		// ...so the apiserver is not told when old history goes: a list
		// continued from its memory would outlive it. Continue from etcd.
		"--feature-gates=ListFromCacheSnapshot=false",
		"--bind-address=127.0.0.1", fmt.Sprintf("--secure-port=%d", t.internalPort),
		"--tls-cert-file=" + t.file("apiserver.crt"), "--tls-private-key-file=" + t.file("apiserver.key"),
		"--client-ca-file=" + t.file("ca.crt"),
		"--kubelet-client-certificate=" + t.file("kubelet-client.crt"), "--kubelet-client-key=" + t.file("kubelet-client.key"),
		"--service-account-key-file=" + t.file("sa.pub"),
		"--service-account-signing-key-file=" + t.file("sa.key"),
		"--service-account-issuer=https://kubernetes.default.svc",
		// pods fetch the keys through the service, not at this process's own port
		"--service-account-jwks-uri=https://kubernetes.default.svc/openid/v1/jwks",
		// extension API servers (aggregation)
		"--requestheader-client-ca-file=" + t.file("front-ca.crt"),
		"--requestheader-allowed-names=front-proxy-client",
		"--requestheader-username-headers=X-Remote-User",
		"--requestheader-group-headers=X-Remote-Group",
		"--requestheader-extra-headers-prefix=X-Remote-Extra-",
		"--proxy-client-cert-file=" + t.file("front-proxy-client.crt"),
		"--proxy-client-key-file=" + t.file("front-proxy-client.key"),
		"--authorization-mode=Node,RBAC",
		"--enable-admission-plugins=NodeRestriction",
		"--enable-bootstrap-token-auth",
		"--allow-privileged=true", // needed by node add-ons such as kube-proxy
		// reach nodes by address: their host names mean nothing outside their network
		"--kubelet-preferred-address-types=InternalIP,ExternalIP,Hostname",
		"--service-cluster-ip-range=" + s.serviceCIDR,
	}
	if s.nodeAddress != "" {
		args = append(args, "--endpoint-reconciler-type=none") // nostromo publishes it, see publishEndpoints
	}
	if t.tunnelEnabled() {
		if err := t.prepareTunnel(); err != nil {
			return err
		}
		args = append(args, "--egress-selector-config-file="+t.file("egress.yaml"),
			"--api-audiences=https://kubernetes.default.svc,"+tunnelAudience)
	}
	t.api, err = start(t.file("apiserver.log"), s.childEnv, filepath.Join(s.tools, "k8s", "kube-apiserver"), args...)
	if err != nil {
		return err
	}
	if err := t.waitReady(60 * time.Second); err != nil {
		t.api.stop()
		t.api = nil
		return err
	}
	t.wakes++
	t.lastRenewal = time.Now()
	t.lastWakeMs = time.Since(begin).Milliseconds()
	t.lastActive = time.Now()
	log.Printf("[%s] awake in %d ms", t.Name, t.lastWakeMs)
	if s.nodeAddress != "" {
		t.publishEndpoints()
	}
	if t.tunnelEnabled() {
		t.startTunnel()
	}
	// the engines start right after, from the supervisor: whoever is waiting
	// for an answer does not wait for them
	t.justWoke = true
	return nil
}

func (t *tenant) waitReady(max time.Duration) error {
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		if !t.api.alive() {
			return errors.New("apiserver exited during startup (see apiserver.log)")
		}
		var body string
		if err := t.get("/readyz", &body); err == nil && body == "ok" {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("apiserver not ready in time")
}

func (t *tenant) enginesRunning() bool { return t.cm.alive() || t.sc.alive() }

// startEngines starts controller-manager and scheduler. With t.mu held.
func (t *tenant) startEngines(reason string) {
	if !t.api.alive() {
		return
	}
	s := t.srv
	k8s := filepath.Join(s.tools, "k8s")
	if !t.cm.alive() {
		if err := t.internalKubeconfig("cm"); err == nil {
			p, _ := freePort()
			kc := t.file("cm.kubeconfig")
			t.cm, err = start(t.file("cm.log"), s.childEnv, filepath.Join(k8s, "kube-controller-manager"),
				"--kubeconfig="+kc, "--authentication-kubeconfig="+kc, "--authorization-kubeconfig="+kc,
				"--service-account-private-key-file="+t.file("sa.key"),
				"--root-ca-file="+t.file("ca.crt"),
				"--cluster-signing-cert-file="+t.file("ca.crt"), "--cluster-signing-key-file="+t.file("ca.key"),
				"--use-service-account-credentials", "--leader-elect=false",
				"--controllers=*,bootstrapsigner,tokencleaner",
				// A stock cluster declares a node lost after 50 seconds of
				// silence. Here the wait is split in two: nostromo notices the
				// silence and starts the engines, the engines wait the rest.
				fmt.Sprintf("--node-monitor-grace-period=%s", s.nodeGrace),
				// Some repairs are made only on a periodic pass (five minutes
				// on a stock cluster): it must come before the engines stop.
				fmt.Sprintf("--resource-quota-sync-period=%s", min(max(s.quiet/2, 5*time.Second), 5*time.Minute)),
				"--bind-address=127.0.0.1", fmt.Sprintf("--secure-port=%d", p),
				"--cert-dir="+t.file("certs-cm"))
			if err != nil {
				log.Printf("[%s] controller-manager: %v", t.Name, err)
			}
		}
	}
	if !t.sc.alive() {
		if err := t.internalKubeconfig("sched"); err == nil {
			p, _ := freePort()
			kc := t.file("sched.kubeconfig")
			t.sc, err = start(t.file("sched.log"), s.childEnv, filepath.Join(k8s, "kube-scheduler"),
				"--kubeconfig="+kc, "--authentication-kubeconfig="+kc, "--authorization-kubeconfig="+kc,
				"--leader-elect=false",
				"--bind-address=127.0.0.1", fmt.Sprintf("--secure-port=%d", p),
				"--cert-dir="+t.file("certs-sched"))
			if err != nil {
				log.Printf("[%s] scheduler: %v", t.Name, err)
			}
		}
	}
	t.enginesStartedAt = time.Now()
	t.engineStarts++
	t.lastReason = reason
	if s.verbose {
		log.Printf("[%s] engines started (%s)", t.Name, reason)
	}
}

// stopEngines stops controller-manager and scheduler. With t.mu held.
func (t *tenant) stopEngines() {
	defer t.releaseLock()
	if t.cm == nil && t.sc == nil {
		return
	}
	var wg sync.WaitGroup
	for _, p := range []*process{t.cm, t.sc} {
		wg.Add(1)
		go func() { defer wg.Done(); p.stop() }()
	}
	wg.Wait()
	t.cm, t.sc = nil, nil
	t.enginesStoppedAt = time.Now()
}

// releaseLock lets another instance run this cluster's engines.
func (t *tenant) releaseLock() {
	if !t.holdsLock {
		return
	}
	t.holdsLock = false
	if err := t.srv.etcd.delIfValue(enginesPrefix+t.Name, []byte(t.srv.instance)); err != nil {
		log.Printf("[%s] releasing the engines: %v", t.Name, err)
	}
}

// sleep stops everything. With t.mu held.
func (t *tenant) sleep() {
	t.stopEngines()
	t.tunnel.stop()
	t.tunnel = nil
	t.api.stop()
	t.api = nil
	t.warming = false
	t.nodesMu.Lock()
	t.nodes = map[string]*node{}
	t.nodesMu.Unlock()
}

// forward relays a connection to the apiserver, waking it up if needed.
// Traffic stays encrypted end to end: nostromo never opens it.
// forward relays a connection to the cluster's apiserver or, for a node's
// tunnel agent, to its tunnel server, waking the cluster first if needed.
func (t *tenant) forward(c net.Conn, consumed []byte, agent bool) {
	defer c.Close()
	t.mu.Lock()
	err := t.wake()
	port := t.internalPort
	if agent {
		port = t.tunnelPort
	}
	if err == nil {
		t.conns++
		t.warming = false
	}
	t.mu.Unlock()
	if err != nil {
		log.Printf("[%s] wake failed: %v", t.Name, err)
		return
	}
	defer func() {
		t.mu.Lock()
		t.conns--
		t.lastActive = time.Now()
		t.mu.Unlock()
	}()
	b, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	for i := 0; err != nil && agent && i < 20; i++ { // the tunnel server may still be starting
		time.Sleep(250 * time.Millisecond)
		t.mu.Lock()
		port = t.tunnelPort
		t.mu.Unlock()
		b, err = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
	}
	if err != nil {
		return
	}
	defer b.Close()
	if len(consumed) > 0 {
		if _, err := b.Write(consumed); err != nil {
			return
		}
	}
	pipe(c, b)
}

// --- what nostromo learns by watching writes in etcd ---

// heartbeat records a node's sign of life (or its removal).
func (t *tenant) heartbeat(name string, deleted bool) {
	t.nodesMu.Lock()
	defer t.nodesMu.Unlock()
	if deleted {
		delete(t.nodes, name)
		return
	}
	n := t.nodes[name]
	if n == nil {
		n = &node{}
		t.nodes[name] = n
	}
	n.heartbeat = time.Now()
	n.reported = false
}

// nodeStatusChanged tells whether a node status write is real news or just
// the usual "I am alive" (which changes a timestamp but not the size).
func (t *tenant) nodeStatusChanged(name string, ev etcdEvent) bool {
	t.nodesMu.Lock()
	defer t.nodesMu.Unlock()
	if ev.deleted {
		return true
	}
	n := t.nodes[name]
	if n == nil {
		n = &node{heartbeat: time.Now()}
		t.nodes[name] = n
	}
	changed := ev.created || n.statusSize != ev.valueLen
	n.statusSize = ev.valueLen
	return changed
}

// silentNode returns a node that has shown no sign of life for too long and
// for which the engines have not been woken yet.
func (t *tenant) silentNode(grace time.Duration) string {
	t.nodesMu.Lock()
	defer t.nodesMu.Unlock()
	for name, n := range t.nodes {
		if !n.reported && time.Since(n.heartbeat) > grace {
			n.reported = true
			return name
		}
	}
	return ""
}

// nodeInTrouble tells whether some node went silent recently. While that is
// so the engines must stay up: they are the ones that mark the node as not
// ready and, minutes later, move its pods elsewhere.
func (t *tenant) nodeInTrouble(grace, window time.Duration) bool {
	t.nodesMu.Lock()
	defer t.nodesMu.Unlock()
	for _, n := range t.nodes {
		if d := time.Since(n.heartbeat); d > grace && d < window {
			return true
		}
	}
	return false
}

func (t *tenant) nodeCount() int {
	t.nodesMu.Lock()
	defer t.nodesMu.Unlock()
	return len(t.nodes)
}

// hasTimedWork tells whether the cluster holds something that must happen
// "on the clock" even when nobody writes: in that case the engines stay on.
// When in doubt it answers yes. With t.mu held.
func (t *tenant) hasTimedWork() bool {
	type list struct {
		Metadata struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
		Items []struct {
			Spec struct {
				TTL *int `json:"ttlSecondsAfterFinished"`
			} `json:"spec"`
			Status struct {
				Conditions []struct {
					Type, Status string
				} `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	for _, p := range []string{"/apis/batch/v1/cronjobs?limit=1", "/apis/autoscaling/v2/horizontalpodautoscalers?limit=1"} {
		var l list
		if err := t.get(p, &l); err != nil || len(l.Items) > 0 {
			return true
		}
	}
	var l list
	if err := t.get("/apis/batch/v1/jobs?limit=500", &l); err != nil || l.Metadata.Continue != "" {
		return true
	}
	for _, j := range l.Items {
		finished := false
		for _, c := range j.Status.Conditions {
			if (c.Type == "Complete" || c.Type == "Failed") && c.Status == "True" {
				finished = true
			}
		}
		if !finished || j.Spec.TTL != nil { // running, or waiting to be cleaned up at its deadline
			return true
		}
	}
	return false
}
