// nostromo hosts many Kubernetes control planes in a single program and keeps
// running, for each of them, only what is needed right now.
//
//	nostromo serve              start the service
//	nostromo datastore          run only the shared datastore (for several instances)
//	nostromo create NAME        create a cluster
//	nostromo kubeconfig NAME    print the file to connect with
//	nostromo join NAME          print the command that joins a worker node
//	nostromo list               show clusters and their level
//	nostromo status             the same, in full, as JSON
//	nostromo delete NAME        delete a cluster and its data
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "serve":
		serve(os.Args[2:])
	case "datastore":
		datastore(os.Args[2:])
	case "create", "kubeconfig", "join", "list", "status", "delete":
		client(os.Args[1], os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  nostromo serve [options]       start the service (nostromo serve -h for options)
  nostromo datastore [options]   run only the shared datastore, for several instances
  nostromo create NAME           create a cluster
  nostromo kubeconfig NAME       print the file to connect with kubectl
  nostromo join NAME             print the command that joins a worker node
  nostromo list                  show clusters and their level
  nostromo status                the same, in full, as JSON
  nostromo delete NAME           delete a cluster and all its data`)
	os.Exit(2)
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func defaultTools() string { return filepath.Join(os.Getenv("HOME"), "nostromo-tools") }

// datastore runs the shared etcd alone. Several nostromo instances can then
// be pointed at it with -etcd and -pki.
func datastore(args []string) {
	f := flag.NewFlagSet("datastore", flag.ExitOnError)
	dir := f.String("dir", "./datastore", "data directory (its pki/ subdirectory holds the credentials instances need)")
	tools := f.String("tools", defaultTools(), "directory holding k8s/ and etcd/ binaries")
	listen := f.String("listen", "127.0.0.1:12379", "address the datastore listens on; instances on other machines need one they can reach")
	f.Parse(args)
	abs, err := filepath.Abs(*dir)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		log.Fatal(err)
	}
	e, err := startEtcd(abs, *tools, *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("datastore ready on %s, credentials in %s", e.url, e.dir)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go e.keepFresh(ctx)
	select {
	case <-ctx.Done():
	case <-e.proc.done:
		log.Print("etcd exited (see etcd.log)")
	}
	e.proc.stop()
}

func serve(args []string) {
	f := flag.NewFlagSet("serve", flag.ExitOnError)
	dir := f.String("dir", "./data", "data directory")
	tools := f.String("tools", defaultTools(), "directory holding k8s/ and etcd/ binaries")
	listen := f.String("listen", "127.0.0.1:6443", "shared port users of every cluster connect to; its host is also used for the clusters' dedicated ports")
	address := f.String("address", "127.0.0.1", "public address (IP or name) written into user kubeconfigs")
	nodeAddress := f.String("node-address", "", "IP address worker nodes and pods use to reach nostromo (not a loopback one); needed for nodes")
	ports := f.String("ports", "30000-39999", "range of dedicated ports, one per cluster")
	domain := f.String("domain", "nostromo.local", "clusters are named NAME.domain")
	admin := f.String("admin", "127.0.0.1:7070", "management API address (TLS; the token and the authority to trust are in the data directory)")
	cidr := f.String("service-cidr", "10.96.0.0/12", "service network inside every cluster")
	podCIDR := f.String("pod-cidr", "10.244.0.0/16", "pod network inside every cluster")
	agentImage := f.String("tunnel-agent-image", "registry.k8s.io/kas-network-proxy/proxy-agent:v0.37.1", "container image of the tunnel agent (Konnectivity) installed on nodes")
	etcdURL := f.String("etcd", "", "use this already running datastore (https://host:port) instead of starting one; lets several instances share the clusters")
	pki := f.String("pki", "", "directory with the datastore credentials (required with -etcd)")
	idle := f.Duration("idle", 5*time.Minute, "with no connections for this long a cluster goes to sleep")
	quiet := f.Duration("quiet", time.Minute, "with no changes for this long the engines (controller-manager, scheduler) stop")
	resync := f.Duration("resync", 10*time.Minute, "how often the engines are started anyway for a safety pass")
	nodeGrace := f.Duration("node-grace", 25*time.Second, "a node silent for this long starts the engines, which then wait as long again before declaring it not ready")
	warm := f.Int("warm", 2, "how many new clusters get their first boot in the background at the same time (0 = boot on first use)")
	always := f.Bool("engines-always", false, "keep the engines always running (no savings on clusters in use)")
	gogc := f.String("gogc", "", "GOGC value for cluster processes (lower = less memory, more CPU)")
	verbose := f.Bool("v", false, "log why the engines start and stop")
	f.Parse(args)

	abs, err := filepath.Abs(*dir)
	if err != nil {
		log.Fatal(err)
	}
	_, network, err := net.ParseCIDR(*cidr)
	if err != nil || network.IP.To4() == nil {
		log.Fatalf("invalid service-cidr: %s", *cidr)
	}
	serviceIP := make(net.IP, 4)
	copy(serviceIP, network.IP.To4())
	serviceIP[3]++ // the first address of the network belongs to the "kubernetes" service

	host, portText, err := net.SplitHostPort(*listen)
	if err != nil {
		log.Fatalf("invalid listen address: %v", err)
	}
	var port, portMin, portMax int
	fmt.Sscan(portText, &port)
	if n, _ := fmt.Sscanf(*ports, "%d-%d", &portMin, &portMax); n != 2 || portMin < 1 || portMax > 65535 || portMin > portMax {
		log.Fatalf("invalid ports range: %s", *ports)
	}
	if *nodeAddress != "" {
		if ip := net.ParseIP(*nodeAddress); ip == nil || ip.To4() == nil || ip.IsLoopback() {
			log.Fatalf("node-address must be a non-loopback IPv4 address: %s", *nodeAddress)
		}
	}
	id := make([]byte, 6)
	rand.Read(id)

	s := &server{dir: abs, tools: *tools, instance: hex.EncodeToString(id),
		address: *address, nodeAddress: *nodeAddress, listenHost: host, port: port, domain: *domain,
		serviceCIDR: *cidr, podCIDR: *podCIDR, serviceIP: serviceIP, portMin: portMin, portMax: portMax,
		verbose: *verbose, agentImage: *agentImage,
		// under /tmp on purpose: a socket path cannot exceed about 100 characters
		socketDir: filepath.Join("/tmp", fmt.Sprintf("nostromo-%d", os.Getuid()), hex.EncodeToString(id)),
		idle:      *idle, quiet: *quiet, resync: *resync, nodeGrace: *nodeGrace, enginesAlways: *always,
		tenants: map[string]*tenant{}}
	if *warm > 0 {
		s.warmQueue = make(chan *tenant, 100000)
	}
	if *gogc != "" {
		s.childEnv = []string{"GOGC=" + *gogc}
	}
	if err := os.MkdirAll(filepath.Join(abs, "tenants"), 0o700); err != nil {
		log.Fatal(err)
	}
	if *etcdURL != "" {
		if *pki == "" {
			log.Fatal("-etcd needs -pki (the datastore's credentials directory)")
		}
		if s.etcd, err = connectEtcd(*pki, strings.TrimRight(*etcdURL, "/")); err == nil {
			err = s.etcd.setup()
		}
	} else {
		s.etcd, err = startEtcd(abs, *tools, "")
	}
	if err != nil {
		log.Fatal(err)
	}
	die := func(err error) {
		s.etcd.proc.stop()
		log.Fatal(err)
	}
	if s.leaseID, err = s.etcd.grantLease(leaseSeconds); err != nil {
		die(err)
	}
	s.announce()
	if err := s.syncTenants(); err != nil {
		die(err)
	}
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		die(err)
	}
	go s.serveShared(l)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go s.etcd.watch(ctx, s.onEvent, s.onWatchLost)
	go s.keepLease(ctx)
	go s.etcd.keepFresh(ctx)
	go s.supervise(ctx)
	for i := 0; i < *warm; i++ {
		go s.warm(ctx)
	}

	token, err := adminToken(abs)
	if err != nil {
		die(err)
	}
	adminHost, _, _ := net.SplitHostPort(*admin)
	adminCfg, err := adminTLS(abs, adminHost, *address)
	if err != nil {
		die(err)
	}
	h := &http.Server{Addr: *admin, Handler: withToken(token, s.adminAPI()), TLSConfig: adminCfg}
	go func() {
		if err := h.ListenAndServeTLS("", ""); err != http.ErrServerClosed {
			log.Printf("admin API: %v", err)
			stop()
		}
	}()
	log.Printf("nostromo ready: instance %s, clusters on %s, admin API on https://%s, %d clusters", s.instance, *listen, *admin, len(s.tenants))

	<-ctx.Done()
	log.Print("shutting down...")
	h.Close()
	l.Close()
	for _, t := range s.list() {
		t.mu.Lock()
		t.sleep()
		t.mu.Unlock()
	}
	s.etcd.proc.stop()
	os.RemoveAll(s.socketDir)
}

// client runs the commands that talk to an already running nostromo.
func client(command string, args []string) {
	f := flag.NewFlagSet(command, flag.ExitOnError)
	admin := f.String("admin", "127.0.0.1:7070", "management API address")
	dir := f.String("dir", envOr("NOSTROMO_DIR", "./data"), "nostromo's data directory, where the API's token and authority are (or NOSTROMO_DIR)")
	name := ""
	if command != "list" && command != "status" {
		if len(args) == 0 || strings.HasPrefix(args[0], "-") {
			usage()
		}
		name, args = args[0], args[1:]
	}
	f.Parse(args)
	base := "https://" + *admin
	cl, token, err := adminClient(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	cl.Timeout = 5 * time.Minute

	do := func(method, path string) []byte {
		r, _ := http.NewRequest(method, base+path, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		res, err := cl.Do(r)
		if err != nil {
			fmt.Fprintln(os.Stderr, "nostromo is not answering on", *admin, "-", err)
			os.Exit(1)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		if res.StatusCode != 200 {
			var e struct{ Error string }
			json.Unmarshal(b, &e)
			fmt.Fprintln(os.Stderr, "error:", e.Error)
			os.Exit(1)
		}
		return b
	}

	switch command {
	case "create":
		do("POST", "/cluster/"+name)
		fmt.Printf("cluster %s created. To connect:\n  nostromo kubeconfig %s > %s.kubeconfig\n  kubectl --kubeconfig %s.kubeconfig get namespaces\n", name, name, name, name)
	case "kubeconfig":
		os.Stdout.Write(do("GET", "/cluster/"+name+"/kubeconfig"))
	case "join":
		var r struct{ Command string }
		json.Unmarshal(do("POST", "/cluster/"+name+"/join"), &r)
		fmt.Println(r.Command)
	case "delete":
		do("DELETE", "/cluster/"+name)
		fmt.Println("cluster", name, "deleted")
	case "status":
		os.Stdout.Write(do("GET", "/status"))
	case "list":
		var st struct {
			ClustersMem int `json:"clusters_mem_mb"`
			NostromoMem int `json:"nostromo_mem_mb"`
			EtcdMem     int `json:"etcd_mem_mb"`
			List        []tenantStatus
		}
		json.Unmarshal(do("GET", "/status"), &st)
		fmt.Printf("%-24s %-8s %6s %5s %6s %8s %6s\n", "CLUSTER", "LEVEL", "PORT", "NODES", "CONNS", "MEM(MB)", "WAKES")
		for _, t := range st.List {
			fmt.Printf("%-24s %-8s %6d %5d %6d %8d %6d\n", t.Name, t.Level, t.Port, t.Nodes, t.Connections, t.ApiMB+t.EnginesMB, t.Wakes)
		}
		fmt.Printf("\nmemory: clusters %d MB, nostromo %d MB, datastore %d MB\n", st.ClustersMem, st.NostromoMem, st.EtcdMem)
	}
}
