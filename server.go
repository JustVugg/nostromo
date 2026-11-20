package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type server struct {
	dir, tools  string
	instance    string // unique name of this running copy of nostromo
	address     string // public address written into user kubeconfigs
	nodeAddress string // address nodes and pods use (never loopback); empty = nodes not supported
	socketDir   string // short path for the local sockets between apiservers and tunnel servers
	agentImage  string // container image of the tunnel agent installed on nodes
	listenHost  string
	port        int    // port shared by all clusters (users)
	domain      string // clusters are named <name>.<domain>
	serviceCIDR string
	podCIDR     string
	serviceIP   net.IP
	portMin     int
	portMax     int
	childEnv    []string
	verbose     bool

	idle          time.Duration // no connections for this long: the cluster goes to sleep
	quiet         time.Duration // no writes for this long: the engines stop
	resync        time.Duration // the engines are started anyway this often, as a safety pass
	nodeGrace     time.Duration // a node silent for this long starts the engines
	enginesAlways bool          // engines always on (no savings)

	etcd *etcd

	leaseMu sync.Mutex
	leaseID string

	portMu   sync.Mutex
	portFree []int // ports believed free, in order; nil = not read yet

	// how many copies of nostromo share the datastore: every node's tunnel
	// agent must end up connected to each of them
	instances atomic.Int32

	warmQueue chan *tenant // clusters waiting for their first boot

	mu      sync.Mutex
	tenants map[string]*tenant
}

// tenantMeta is a cluster's definition as stored in the datastore. It is
// written once at creation and never changes, so every instance of nostromo
// can serve the cluster with the same identity.
type tenantMeta struct {
	Name string `json:"name"`
	Port int    `json:"port"`
	// AgentPort is where the cluster's nodes keep a tunnel open, so that the
	// control plane can reach them wherever they are (Konnectivity).
	AgentPort int    `json:"agent_port"`
	CACrt     string `json:"ca_crt"`
	CAKey     string `json:"ca_key"`
	SAKey     string `json:"sa_key"`
	SAPub     string `json:"sa_pub"`
}

var validName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$`)

func (s *server) lease() string {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	return s.leaseID
}

// leaseSeconds is how long an instance's claims survive it. An instance that
// cannot renew keeps trying (a busy disk can stall etcd for many seconds) but
// gives up, and stops its engines, before another instance may start them:
// at worst leaseGiveUp + one tick + keepAliveTimeout after the last renewal.
const (
	leaseSeconds     = 30
	leaseTick        = 5 * time.Second
	keepAliveTimeout = 4 * time.Second
	leaseGiveUp      = 20 * time.Second
)

// keepLease renews this instance's lease. Everything this instance has
// claimed in the datastore (the engines of some clusters) is tied to it: if
// the instance dies, the claims expire and another instance takes over.
func (s *server) keepLease(ctx context.Context) {
	tick := time.NewTicker(leaseTick)
	defer tick.Stop()
	renewed := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		ok, gone := s.etcd.keepAlive(s.lease())
		if ok {
			renewed = time.Now()
			continue
		}
		if !gone && time.Since(renewed) < leaseGiveUp {
			continue // the lease is still ours for a while: try again
		}
		log.Print("lease lost: giving up the engines this instance was running")
		for _, t := range s.list() {
			t.mu.Lock()
			if t.holdsLock {
				t.holdsLock = false // already gone with the lease
				t.stopEngines()
				t.owed = true // their work is not done: resume as soon as possible
			}
			t.mu.Unlock()
		}
		if id, err := s.etcd.grantLease(leaseSeconds); err == nil {
			s.leaseMu.Lock()
			s.leaseID = id
			s.leaseMu.Unlock()
			renewed = time.Now()
			s.announce()
		}
	}
}

// announce records this instance among the running ones, for as long as its
// lease lives.
func (s *server) announce() {
	if _, _, err := s.etcd.putIfAbsent(instancesPrefix+s.instance, []byte(s.instance), s.lease()); err != nil {
		log.Printf("announcing this instance: %v", err)
	}
	s.countInstances()
}

// countInstances looks at how many instances are running. When the number
// changes, the tunnel servers are restarted with the new one: it is from
// them that the nodes' agents learn how many servers to look for.
func (s *server) countInstances() {
	l, err := s.etcd.list(instancesPrefix)
	if err != nil {
		return
	}
	n := int32(max(len(l), 1))
	if s.instances.Swap(n) == n {
		return
	}
	log.Printf("%d instance(s) of nostromo share the datastore", n)
	for _, t := range s.list() {
		t.mu.Lock()
		if t.tunnel != nil {
			t.tunnel.stop()
			t.startTunnel()
		}
		t.mu.Unlock()
	}
}

// reservePort picks a free dedicated port and reserves it for the cluster.
func (s *server) reservePort(name string) (int, error) {
	s.portMu.Lock()
	defer s.portMu.Unlock()
	// Reading every reservation at each creation would make creation slower
	// the more clusters there are. The list is read once, then ports are
	// tried in order from where the last search stopped; the datastore
	// refuses a port someone else holds. Ports freed by deleted clusters are
	// found again by a fresh read when the range has been walked to its end.
	for pass := 0; pass < 2; pass++ {
		if s.portFree == nil {
			taken, err := s.etcd.list(portsPrefix)
			if err != nil {
				return 0, err
			}
			used := map[int]bool{}
			for _, k := range taken {
				p, _ := strconv.Atoi(strings.TrimPrefix(k.key, portsPrefix))
				used[p] = true
			}
			s.portFree = []int{}
			for p := s.portMin; p <= s.portMax; p++ {
				if !used[p] {
					s.portFree = append(s.portFree, p)
				}
			}
		}
		for len(s.portFree) > 0 {
			p := s.portFree[0]
			s.portFree = s.portFree[1:]
			ok, _, err := s.etcd.putIfAbsent(portsPrefix+strconv.Itoa(p), []byte(name), "")
			if err != nil {
				return 0, err
			}
			if ok { // otherwise another instance took it meanwhile: try the next
				return p, nil
			}
		}
		s.portFree = nil // walked to the end: look again, once
	}
	return 0, fmt.Errorf("no free port left in %d-%d", s.portMin, s.portMax)
}

func (s *server) create(name string) (*tenant, error) {
	if !validName.MatchString(name) || name == "root" {
		return nil, errors.New("invalid name: lowercase letters, digits and dashes (40 characters at most)")
	}
	if s.find(name) != nil {
		return nil, errors.New("already exists")
	}
	ca, err := newCA("nostromo-" + name)
	if err != nil {
		return nil, err
	}
	sa, saPEM, err := newKey()
	if err != nil {
		return nil, err
	}
	pub, err := x509.MarshalPKIXPublicKey(&sa.PublicKey)
	if err != nil {
		return nil, err
	}
	port, err := s.reservePort(name)
	if err != nil {
		return nil, err
	}
	agentPort, err := s.reservePort(name)
	if err != nil {
		s.etcd.delIfValue(portsPrefix+strconv.Itoa(port), []byte(name))
		return nil, err
	}
	meta := tenantMeta{Name: name, Port: port, AgentPort: agentPort, CACrt: string(ca.certPEM), CAKey: string(ca.keyPEM),
		SAKey: string(saPEM), SAPub: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub}))}
	doc, _ := json.Marshal(meta)
	ok, _, err := s.etcd.putIfAbsent(tenantsPrefix+name, doc, "")
	if err != nil || !ok {
		s.etcd.delIfValue(portsPrefix+strconv.Itoa(port), []byte(name))
		s.etcd.delIfValue(portsPrefix+strconv.Itoa(agentPort), []byte(name))
		if err == nil {
			err = errors.New("already exists")
		}
		return nil, err
	}
	if err := s.etcd.addCluster(name); err != nil {
		return nil, err
	}
	log.Printf("[%s] created (port %d)", name, port)
	t, err := s.loadTenant(meta)
	if err == nil && s.warmQueue != nil {
		select {
		case s.warmQueue <- t:
		default: // queue full: the first user connection will do the first boot
		}
	}
	return t, err
}

// warm gives new clusters their first boot in the background. The first boot
// is the slow one (the cluster fills in its initial state); done here, nobody
// has to wait for it. Once settled, the cluster goes back to sleep. Each
// worker handles one cluster at a time, so memory use stays bounded however
// many clusters are created at once.
func (s *server) warm(ctx context.Context) {
	for {
		var t *tenant
		select {
		case <-ctx.Done():
			return
		case t = <-s.warmQueue:
		}
		t.mu.Lock()
		if t.wakes == 0 && s.find(t.Name) == t {
			t.warming = true
			if err := t.wake(); err != nil {
				t.warming = false
				log.Printf("[%s] first boot: %v", t.Name, err)
			}
		}
		t.mu.Unlock()
		for ctx.Err() == nil { // until it is asleep again, or someone started using it
			time.Sleep(500 * time.Millisecond)
			t.mu.Lock()
			busy := t.warming && t.api.alive()
			t.mu.Unlock()
			if !busy {
				break
			}
		}
	}
}

// loadTenant makes a cluster servable by this instance: it writes the
// cluster's identity to the local disk, issues this instance's certificates
// and opens the dedicated port. Safe to call more than once.
func (s *server) loadTenant(m tenantMeta) (*tenant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.tenants[m.Name]; t != nil {
		return t, nil
	}
	if !validName.MatchString(m.Name) {
		return nil, errors.New("invalid cluster record: " + m.Name)
	}
	t := &tenant{Name: m.Name, Port: m.Port, AgentPort: m.AgentPort, dir: filepath.Join(s.dir, "tenants", m.Name), srv: s, nodes: map[string]*node{}}
	if err := os.MkdirAll(t.dir, 0o700); err != nil {
		return nil, err
	}
	for name, content := range map[string]string{"ca.crt": m.CACrt, "ca.key": m.CAKey, "sa.key": m.SAKey, "sa.pub": m.SAPub} {
		if old, err := os.ReadFile(t.file(name)); err == nil && string(old) == content {
			continue
		}
		if err := writeAtomic(t.file(name), []byte(content)); err != nil {
			return nil, err
		}
	}
	if err := s.frontCA(t); err != nil {
		return nil, err
	}
	if err := t.prepare(); err != nil {
		return nil, err
	}
	if t.Port > 0 {
		l, err := net.Listen("tcp", net.JoinHostPort(s.listenHost, strconv.Itoa(t.Port)))
		if err != nil {
			// still reachable through the shared port
			log.Printf("[%s] dedicated port %d not available here: %v", t.Name, t.Port, err)
		} else {
			t.listener = l
			go func() {
				for {
					c, err := l.Accept()
					if err != nil {
						return
					}
					go t.forward(c, nil, false)
				}
			}()
		}
	}
	if t.tunnelEnabled() {
		// Agents are answered by nostromo, which hands them to the cluster's
		// tunnel server, waking the cluster first if it sleeps here.
		l, err := net.Listen("tcp", net.JoinHostPort(s.listenHost, strconv.Itoa(t.AgentPort)))
		if err != nil {
			log.Printf("[%s] agent port %d not available here: %v", t.Name, t.AgentPort, err)
		} else {
			t.agentListener = l
			go func() {
				for {
					c, err := l.Accept()
					if err != nil {
						return
					}
					go t.forward(c, nil, true)
				}
			}()
		}
	}
	s.tenants[t.Name] = t
	return t, nil
}

// unloadTenant stops serving a cluster from this instance and forgets it.
func (s *server) unloadTenant(name string) {
	s.mu.Lock()
	t := s.tenants[name]
	delete(s.tenants, name)
	s.mu.Unlock()
	if t == nil {
		return
	}
	if t.listener != nil {
		t.listener.Close()
	}
	if t.agentListener != nil {
		t.agentListener.Close()
	}
	t.mu.Lock()
	t.sleep()
	t.mu.Unlock()
	os.RemoveAll(t.dir)
	log.Printf("[%s] removed from this instance", name)
}

func (s *server) remove(name string) error {
	t := s.find(name)
	if t == nil {
		return errors.New("not found")
	}
	// Removing the record makes every instance stop serving the cluster...
	if err := s.etcd.del(tenantsPrefix + name); err != nil {
		return err
	}
	s.unloadTenant(name)
	// ...then, once they have had the time to stop, the data goes too.
	time.Sleep(1500 * time.Millisecond)
	if err := s.etcd.removeCluster(name); err != nil {
		return err
	}
	s.etcd.del(enginesPrefix + name)
	s.etcd.del(frontPrefix + name)
	s.etcd.delIfValue(portsPrefix+strconv.Itoa(t.AgentPort), []byte(name))
	return s.etcd.delIfValue(portsPrefix+strconv.Itoa(t.Port), []byte(name))
}

// frontCA puts in the cluster's directory the CA that vouches for the
// apiserver when it forwards a request to an extension API server. It is not
// the cluster's CA: a certificate signed by this one can speak in the name of
// any user. Every instance must use the same one, so the first to need it
// writes it in the datastore and the others take it from there.
func (s *server) frontCA(t *tenant) error {
	type record struct {
		Crt string `json:"crt"`
		Key string `json:"key"`
	}
	ca, err := newCA("nostromo-" + t.Name + "-front-proxy")
	if err != nil {
		return err
	}
	mine, _ := json.Marshal(record{string(ca.certPEM), string(ca.keyPEM)})
	ok, theirs, err := s.etcd.putIfAbsent(frontPrefix+t.Name, mine, "")
	if err != nil {
		return err
	}
	if !ok {
		mine = theirs
	}
	var r record
	if err := json.Unmarshal(mine, &r); err != nil || r.Crt == "" || r.Key == "" {
		return errors.New("invalid front-proxy record for " + t.Name)
	}
	for name, content := range map[string]string{"front-ca.crt": r.Crt, "front-ca.key": r.Key} {
		if old, err := os.ReadFile(t.file(name)); err == nil && string(old) == content {
			continue
		}
		if err := writeAtomic(t.file(name), []byte(content)); err != nil {
			return err
		}
		os.Remove(t.file("front-proxy-client.crt")) // signed by a CA that is no longer the one
	}
	return nil
}

// syncTenants aligns this instance with the cluster records in the datastore.
func (s *server) syncTenants() error {
	records, err := s.etcd.list(tenantsPrefix)
	if err != nil {
		return err
	}
	wanted := map[string]bool{}
	for _, r := range records {
		var m tenantMeta
		if json.Unmarshal(r.value, &m) != nil || tenantsPrefix+m.Name != r.key {
			continue
		}
		wanted[m.Name] = true
		if _, err := s.loadTenant(m); err != nil {
			log.Printf("[%s] cannot load: %v", m.Name, err)
		}
	}
	for _, t := range s.list() {
		if !wanted[t.Name] {
			s.unloadTenant(t.Name)
		}
	}
	return nil
}

func (s *server) find(name string) *tenant {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tenants[name]
}

func (s *server) list() []*tenant {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts := make([]*tenant, 0, len(s.tenants))
	for _, t := range s.tenants {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(a, b int) bool { return ts[a].Name < ts[b].Name })
	return ts
}

// serveShared is the single port: it reads which cluster the connection is
// meant for (the name in the TLS hello) and relays it to the right one.
func (s *server) serveShared(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			return
		}
		go func() {
			name, consumed, err := readSNI(c)
			if err != nil {
				c.Close()
				return
			}
			cluster, ok := strings.CutSuffix(name, "."+s.domain)
			t := s.find(cluster)
			if !ok || t == nil {
				c.Close()
				return
			}
			t.forward(c, consumed, false)
		}()
	}
}

// onEvent receives every write of every cluster and decides whether it is
// "real work". Periodic signs of life (leases, events, unchanged node status)
// are not. It also follows nostromo's own records, so that clusters created
// or deleted through another instance appear and disappear here too.
func (s *server) onEvent(ev etcdEvent) {
	if name, ok := strings.CutPrefix(ev.key, tenantsPrefix); ok {
		if ev.deleted {
			go s.unloadTenant(name)
			return
		}
		var m tenantMeta
		if json.Unmarshal(ev.value, &m) == nil && m.Name == name {
			go func() {
				if _, err := s.loadTenant(m); err != nil {
					log.Printf("[%s] cannot load: %v", name, err)
				}
			}()
		}
		return
	}
	if strings.HasPrefix(ev.key, instancesPrefix) {
		go s.countInstances()
		return
	}
	rest, ok := strings.CutPrefix(ev.key, rootPrefix)
	if !ok {
		return
	}
	name, path, ok := strings.Cut(rest, "/")
	if !ok {
		return
	}
	t := s.find(name)
	if t == nil {
		return
	}
	resource, tail, _ := strings.Cut(path, "/")
	switch resource {
	case "events", "masterleases", "peerserverleases": // periodic noise, not work
		return
	case "leases":
		if n, ok := strings.CutPrefix(tail, "kube-node-lease/"); ok {
			t.heartbeat(n, ev.deleted)
		}
		return
	case "minions": // node objects
		if !t.nodeStatusChanged(tail, ev) {
			return
		}
	}
	t.lastWrite.Store(time.Now().UnixNano())
	if s.verbose {
		log.Printf("[%s] write: %s", name, path)
	}
}

// onWatchLost: we do not know what happened meanwhile, so the cluster list is
// read again and every cluster is treated as just modified.
func (s *server) onWatchLost() {
	if err := s.syncTenants(); err != nil {
		log.Printf("reading the cluster list: %v", err)
	}
	s.countInstances()
	now := time.Now().UnixNano()
	for _, t := range s.list() {
		t.lastWrite.Store(now)
	}
}

// supervise decides, for every cluster, which level it should be at.
func (s *server) supervise(ctx context.Context) {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		for _, t := range s.list() {
			if !t.mu.TryLock() { // waking up: come back later
				continue
			}
			s.adjust(t)
			t.mu.Unlock()
		}
	}
}

// nodeTroubleWindow is how long the engines are kept up after a node goes
// silent: Kubernetes waits 5 minutes before moving the pods of a lost node.
const nodeTroubleWindow = 8 * time.Minute

func (s *server) adjust(t *tenant) {
	now := time.Now()
	if !t.api.alive() {
		if t.api != nil || t.enginesRunning() { // apiserver died on its own
			log.Printf("[%s] apiserver exited unexpectedly (see apiserver.log)", t.Name)
			t.sleep()
		}
		return
	}
	// a cluster booted only to initialise it goes back to sleep as soon as
	// its engines have done their first round
	settled := t.warming && !t.justWoke && t.engineStarts > 0 && !t.enginesRunning()
	if s.enginesAlways {
		settled = t.warming && now.Sub(t.lastActive) > s.quiet
	}
	if t.conns == 0 && (now.Sub(t.lastActive) > s.idle || settled) {
		t.sleep()
		log.Printf("[%s] asleep", t.Name)
		return
	}
	// A cluster with nodes never sleeps: expiring certificates are renewed
	// here, once a day (the processes reload the files on their own).
	if now.Sub(t.lastRenewal) > renewalCheck {
		t.lastRenewal = now
		before, _ := os.ReadFile(t.file("apiserver.crt"))
		if err := t.prepare(); err != nil {
			log.Printf("[%s] certificate renewal: %v", t.Name, err)
		}
		// the tunnel server reads its certificate only when it starts
		if after, _ := os.ReadFile(t.file("apiserver.crt")); t.tunnel != nil && !bytes.Equal(before, after) {
			t.tunnel.stop()
			t.startTunnel()
		}
	}
	if t.tunnel != nil && !t.tunnel.alive() { // the tunnel server died: bring it back
		log.Printf("[%s] tunnel server exited (see konnectivity.log), restarting", t.Name)
		t.startTunnel()
	}
	written := time.Unix(0, t.lastWrite.Load())
	if t.enginesRunning() {
		if !t.cm.alive() || !t.sc.alive() { // one of the two died: bring it back
			t.startEngines("recovery")
			return
		}
		if s.enginesAlways || now.Sub(written) < s.quiet || now.Sub(t.enginesStartedAt) < s.quiet ||
			now.Before(t.nextTimedCheck) || t.nodeInTrouble(s.nodeGrace, nodeTroubleWindow) {
			return
		}
		if t.hasTimedWork() {
			t.nextTimedCheck = now.Add(time.Minute)
			return
		}
		t.stopEngines()
		if s.verbose {
			log.Printf("[%s] engines stopped", t.Name)
		}
		return
	}
	if now.Before(t.nextLockTry) {
		return
	}
	reason := ""
	switch {
	case t.justWoke:
		reason = "wake"
	case t.owed:
		reason = "resuming"
	case s.enginesAlways:
		reason = "always on"
	case written.After(t.enginesStoppedAt):
		reason = "write"
	case now.Sub(t.enginesStoppedAt) > s.resync:
		reason = "safety pass"
	default:
		if n := t.silentNode(s.nodeGrace); n != "" {
			reason = "node " + n + " shows no sign of life"
		}
	}
	if reason == "" {
		return
	}
	// Only one instance at a time runs a cluster's engines.
	ok, holder, err := s.etcd.putIfAbsent(enginesPrefix+t.Name, []byte(s.instance), s.lease())
	if err != nil || (!ok && string(holder) != s.instance) {
		// Someone else is doing the work. Look again shortly: if that
		// instance dies its claim expires and the work falls to us.
		t.nextLockTry = now.Add(2 * time.Second)
		return
	}
	t.holdsLock = true
	t.justWoke = false
	t.owed = false
	t.startEngines(reason)
}

// join prepares the cluster to accept worker nodes and returns the command to
// run on a new node. It relies on kubeadm, the standard Kubernetes tool, so a
// node joins a nostromo cluster exactly like any other cluster.
func (s *server) join(name string) (string, error) {
	t := s.find(name)
	if t == nil {
		return "", errors.New("not found")
	}
	if s.nodeAddress == "" {
		return "", errors.New("nodes need an address to reach nostromo: start it with -node-address")
	}
	if t.listener == nil {
		return "", fmt.Errorf("the cluster's dedicated port %d is not open on this instance", t.Port)
	}
	t.mu.Lock()
	err := t.wake()
	t.lastActive = time.Now()
	t.mu.Unlock()
	if err != nil {
		return "", err
	}
	var version struct {
		GitVersion string `json:"gitVersion"`
	}
	if err := t.get("/version", &version); err != nil {
		return "", err
	}
	kc, err := t.nodeKubeconfig()
	if err != nil {
		return "", err
	}
	kcFile, cfgFile := t.file("node-admin.kubeconfig"), t.file("kubeadm.yaml")
	if err := writeAtomic(kcFile, kc); err != nil {
		return "", err
	}
	cfg := fmt.Sprintf(`apiVersion: kubeadm.k8s.io/v1beta4
kind: ClusterConfiguration
kubernetesVersion: %s
controlPlaneEndpoint: %q
networking:
  serviceSubnet: %q
  podSubnet: %q
  dnsDomain: cluster.local
`, version.GitVersion, t.nodeEndpoint(), s.serviceCIDR, s.podCIDR)
	// kubeadm wants to know "this machine's address" and, left alone, looks
	// for a default route: say it here, so that preparing a cluster does not
	// depend on how this machine reaches the internet.
	advertise := net.ParseIP(s.nodeAddress)
	if advertise == nil {
		if ips, err := net.LookupIP(s.nodeAddress); err == nil && len(ips) > 0 {
			advertise = ips[0]
		}
	}
	if advertise != nil {
		cfg += fmt.Sprintf(`---
apiVersion: kubeadm.k8s.io/v1beta4
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: %q
  bindPort: %d
`, advertise.String(), t.Port)
	}
	if err := writeAtomic(cfgFile, []byte(cfg)); err != nil {
		return "", err
	}
	kubeadm := filepath.Join(s.tools, "k8s", "kubeadm")
	run := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		var out, errOut bytes.Buffer
		cmd := exec.CommandContext(ctx, kubeadm, args...)
		cmd.Stdout, cmd.Stderr = &out, &errOut
		if err := cmd.Run(); err != nil {
			return "", fmt.Errorf("kubeadm %s: %v: %s", strings.Join(args[:3], " "), err, lastLines(errOut.String(), 3))
		}
		return out.String(), nil
	}
	// What a node looks for when it joins: the cluster's settings, the
	// permission for new nodes to request their certificate, and the two
	// standard add-ons (service routing and DNS).
	for _, phase := range [][]string{
		{"init", "phase", "upload-config", "all"},
		{"init", "phase", "bootstrap-token", "--skip-token-print"},
		{"init", "phase", "addon", "all"},
	} {
		if _, err := run(append(phase, "--kubeconfig", kcFile, "--config", cfgFile)...); err != nil {
			return "", err
		}
	}
	if t.tunnelEnabled() {
		if err := t.installTunnelAgent(); err != nil {
			return "", fmt.Errorf("installing the tunnel agent: %w", err)
		}
	}
	out, err := run("token", "create", "--print-join-command", "--ttl", "24h", "--kubeconfig", kcFile)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "kubeadm join") {
			return strings.TrimSpace(line), nil
		}
	}
	return "", errors.New("kubeadm printed no join command")
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}

type tenantStatus struct {
	Name         string `json:"name"`
	Level        string `json:"level"` // asleep | waking | standby | full
	Port         int    `json:"port"`
	AgentPort    int    `json:"agent_port"` // where nodes keep the tunnel open
	Connections  int    `json:"connections"`
	Nodes        int    `json:"nodes"`
	ApiMB        int    `json:"api_mb"`
	EnginesMB    int    `json:"engines_mb"`
	TunnelMB     int    `json:"tunnel_mb"`
	Wakes        int    `json:"wakes"`
	LastWakeMs   int64  `json:"last_wake_ms"`
	EngineStarts int    `json:"engine_starts"`
	LastReason   string `json:"last_reason,omitempty"`
}

func (s *server) status() map[string]any {
	ts := []tenantStatus{}
	levels := map[string]int{"asleep": 0, "waking": 0, "standby": 0, "full": 0}
	mem := 0
	for _, t := range s.list() {
		if !t.mu.TryLock() { // busy starting or stopping: do not make the caller wait
			levels["waking"]++
			ts = append(ts, tenantStatus{Name: t.Name, Level: "waking", Port: t.Port, AgentPort: t.AgentPort})
			continue
		}
		st := tenantStatus{Name: t.Name, Level: "asleep", Port: t.Port, AgentPort: t.AgentPort, Connections: t.conns, Nodes: t.nodeCount(),
			ApiMB: t.api.rssMB(), EnginesMB: t.cm.rssMB() + t.sc.rssMB(), TunnelMB: t.tunnel.rssMB(), Wakes: t.wakes, LastWakeMs: t.lastWakeMs,
			EngineStarts: t.engineStarts, LastReason: t.lastReason}
		if t.api.alive() {
			st.Level = "standby"
			if t.enginesRunning() {
				st.Level = "full"
			}
		}
		t.mu.Unlock()
		levels[st.Level]++
		mem += st.ApiMB + st.EnginesMB + st.TunnelMB
		ts = append(ts, st)
	}
	return map[string]any{
		"instance": s.instance, "clusters": len(ts), "levels": levels,
		"clusters_mem_mb": mem, "nostromo_mem_mb": rssMB(os.Getpid()), "etcd_mem_mb": s.etcd.proc.rssMB(),
		"list": ts,
	}
}

func (s *server) adminAPI() http.Handler {
	mux := http.NewServeMux()
	fail := func(w http.ResponseWriter, err error, code int) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	}
	mux.HandleFunc("POST /cluster/{name}", func(w http.ResponseWriter, r *http.Request) {
		t, err := s.create(r.PathValue("name"))
		if err != nil {
			fail(w, err, http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"name": t.Name, "port": t.Port})
	})
	mux.HandleFunc("DELETE /cluster/{name}", func(w http.ResponseWriter, r *http.Request) {
		if err := s.remove(r.PathValue("name")); err != nil {
			fail(w, err, http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"deleted": r.PathValue("name")})
	})
	mux.HandleFunc("GET /cluster/{name}/kubeconfig", func(w http.ResponseWriter, r *http.Request) {
		t := s.find(r.PathValue("name"))
		if t == nil {
			fail(w, errors.New("not found"), http.StatusNotFound)
			return
		}
		b, err := t.adminKubeconfig()
		if err != nil {
			fail(w, err, http.StatusInternalServerError)
			return
		}
		w.Write(b)
	})
	mux.HandleFunc("POST /cluster/{name}/join", func(w http.ResponseWriter, r *http.Request) {
		cmd, err := s.join(r.PathValue("name"))
		if err != nil {
			fail(w, err, http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"command": cmd})
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(s.status())
	})
	return mux
}
