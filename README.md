<p align="center">
  <img src="assets/logo.png" alt="nostromo" width="520">
</p>

# nostromo

Hosts many Kubernetes control planes in a single program and keeps running,
for each of them, only what is needed right now.

It is built for whoever sells managed Kubernetes (cloud providers): every
customer's control plane is a fixed cost, and most of the time it does nothing.

## The three levels

| Level | What is running | When |
|---|---|---|
| asleep | nothing | nobody is connected to the cluster |
| standby | the apiserver only | nodes or users are connected, but there is no work to do |
| full | apiserver + engines (controller-manager, scheduler) | something is changing in the cluster |

Moving between levels is automatic:

- **asleep → standby:** on the first connection. Whoever connects waits for
  the wake-up (about 2 seconds) and then works as usual.
- **standby → full:** as soon as something real is written to the cluster (a
  pod, a deployment, a new node...). Node heartbeats do not count.
- **full → standby:** after `-quiet` with no writes, unless the cluster holds
  clock-driven work: CronJobs, autoscalers, running Jobs, Jobs waiting for
  their clean-up deadline, or a node that recently went silent.
- **standby → full, for safety:** if a node stops showing signs of life
  (`-node-grace`), and in any case every `-resync`.
- **→ asleep:** after `-idle` with no connections at all.

A new cluster gets its first boot (the slow one) in the background right after
creation and goes back to sleep, so its first user only waits a normal wake-up.

## What is inside

- **Real Kubernetes**: the upstream binaries, unmodified.
- **One port for every cluster's users**: nostromo reads which cluster a
  connection is meant for (the name in the TLS hello) and relays it to the
  right one. Traffic stays encrypted end to end; nostromo never opens it.
- **One dedicated port per cluster for its nodes and pods**, which reach the
  API by plain address. nostromo tells each cluster which address and port to
  advertise internally (the `kubernetes` service).
- **Standard node join**: `nostromo join NAME` prepares the cluster with
  kubeadm and prints an ordinary `kubeadm join ...` command, with kube-proxy
  and CoreDNS installed.
- **A tunnel towards the nodes (Konnectivity)**: nodes usually sit in the
  customer's network, where the control plane cannot dial them. An agent on
  each node calls nostromo and keeps the connection open; logs, exec and
  webhooks travel through it. nostromo runs the upstream tunnel server next to
  each awake apiserver (about 35 MB) and `nostromo join` installs the agent.
- **One certificate authority per cluster**: one cluster's credentials do not
  open the others. Certificates renew themselves.
- **A shared but partitioned datastore (etcd)**: every cluster has its own key
  and can read and write only its own slice.
- **Several instances**: any number of nostromo copies can share one
  datastore. Each serves every cluster; only one at a time runs a cluster's
  engines, and if it dies another takes over within half a minute.
- **No dependencies**: a single Go binary, standard library only.

## Try it

On any Linux machine (x86-64 or arm64), without root:

```sh
git clone https://github.com/JustVugg/nostromo && cd nostromo
./quickstart.sh
```

It fetches the Kubernetes and etcd binaries (checksums verified) into
`~/nostromo-tools`, plus the Go toolchain if the machine has none, builds the
tunnel server and nostromo, starts it, creates a cluster and shows it
answering `kubectl`: about four minutes the first time, seconds afterwards.
Nothing is installed system-wide; `./quickstart.sh stop --purge` and deleting
`~/nostromo-tools` undo everything.

## Usage

nostromo looks for the binaries in `~/nostromo-tools` (`k8s/kube-apiserver`,
`k8s/kube-controller-manager`, `k8s/kube-scheduler`, `k8s/kubeadm`,
`etcd/etcd`, and for the tunnel `konnectivity/proxy-server`); `-tools` points
it elsewhere. `quickstart.sh` puts them there.

```sh
go build -o nostromo .
./nostromo serve -dir ./data &

./nostromo create alpha
./nostromo kubeconfig alpha > alpha.kubeconfig
kubectl --kubeconfig alpha.kubeconfig get namespaces   # starts on its own

./nostromo list
./nostromo delete alpha
```

`./nostromo serve -h` lists every option (timings, addresses, domain).

The commands talk to the running service through its management API
(`-admin`, 127.0.0.1:7070 by default). The API speaks only TLS and wants a
secret token; both the token and the authority to trust are created in the
data directory (`admin.token`, `admin-ca.crt`), where the commands find them
(`-dir` or `NOSTROMO_DIR`). To manage nostromo from another machine, copy
those two files there.

### Worker nodes

Nodes and pods need a real (non-loopback) address to reach nostromo:

```sh
./nostromo serve -dir ./data -listen 0.0.0.0:6443 -address 203.0.113.10 -node-address 203.0.113.10 &
./nostromo create alpha
./nostromo join alpha        # prints: kubeadm join 203.0.113.10:30000 --token ... --discovery-token-ca-cert-hash sha256:...
```

Each cluster uses two ports from `-ports`: one for its API (nodes and pods)
and one for the tunnel agents. Both must be reachable from the nodes.

Run the printed command as root on a machine with kubelet, kubeadm and a
container runtime installed. A pod network (CNI) is the cluster owner's choice
and is not installed by nostromo.

### Several instances

```sh
./nostromo datastore -dir ./ds -listen 10.0.0.5:12379 &     # or your own etcd cluster
# on machine A and on machine B, each with a copy of ./ds/pki:
./nostromo serve -dir ./data -etcd https://10.0.0.5:12379 -pki ./pki -address LB -node-address LB &
```

Put a TCP load balancer in front of the instances and use its address as
`-address` and `-node-address`. Users, nodes and the nodes' tunnel agents
all call that one address; each agent keeps calling until it is connected to
every instance, so a node is reached whichever instance a request lands on.
`nostromo datastore` runs a single etcd: for a
datastore that survives a machine loss, run a regular multi-member etcd with
the same credentials and point `-etcd` at it. Give that etcd
`--auto-compaction-mode=periodic --auto-compaction-retention=4m`: the
clusters leave the cleaning of old history to the datastore they share.

## Measurements (laptop, Kubernetes v1.37.2, etcd v3.7.4)

| Measurement | Value | Script |
|---|---|---|
| Classic always-on control plane, empty cluster | 405 MB | `bench/baseline.sh` |
| 20 clusters asleep | 0 MB (+16 MB nostromo, +92 MB etcd in total) | `bench/density.sh` |
| Wake-up as seen by kubectl, 7 runs | average 2.5 s, worst 3.0 s | `bench/density.sh` |
| Cluster in use with 2 (fake) nodes, at rest | 263 MB instead of 417 MB | `bench/levels.sh` |
| Cluster with two real nodes and their system pods, at rest | 287 MB instead of 455 MB (tunnel server included) | `bench/realnode/` |
| A real kubelet stopped: node marked not ready | after 50 s, as in a stock cluster | `bench/realnode/` |
| 100 clusters started once, all asleep | 0 MB (+19 MB nostromo, +141 MB etcd, 146 MB on disk) | `bench/scale.sh` |
| 1100 clusters defined (100 started, 1000 only created) | nostromo 25 MB, etcd 106 MB | `bench/scale.sh` |
| Wake-up with 1100 clusters defined, 10 runs | average 2.7 s, worst 2.9 s | `bench/scale.sh` |
| First boot of a new cluster, 100 runs, busy machine | median 5.0 s, 8 of 100 over kubectl's 10 s limit | `bench/scale.sh` |

The scale run shared the machine with another heavy process, so its timings
are pessimistic. It ran with background first boot turned off, to measure the
first boot itself: the 8 slow ones are the case that feature exists for.

With `-gogc 50` the apiserver at rest drops from 274 to 231 MB (with 20:
207 MB), at the cost of more CPU, not measured under load.

## Tests

```sh
go test .                # unit tests
bash bench/e2e.sh        # isolation, sleep, restart, deletion, background first boot (1 minute)
bash bench/levels.sh     # the three levels with fake nodes (4 minutes)
bash bench/nodes.sh      # dedicated port, kubeadm join, a node joining like a kubelet (1 minute)
bash bench/tunnel.sh     # the control plane reaching a node through the tunnel (1 minute)
bash bench/ha.sh         # two instances on one datastore, takeover (2 minutes)
bash bench/tunnel-ha.sh  # two instances behind one address, a node reached through either (2 minutes)
bash bench/certs.sh      # three certificate lifetimes on a cluster that never restarts (7 minutes)
bash bench/density.sh 20 # memory and wake-up times
bash bench/scale.sh      # 100 started clusters plus 1000 created
```

A real worker node (needs root on the node machine, see the scripts' headers):

```sh
bash bench/realnode/start.sh             # nostromo + cluster "demo" + join command
sudo bash bench/realnode/install.sh      # containerd, kubelet, kubeadm join
bash bench/realnode/check.sh             # containers, logs, exec, API and DNS from a pod
sudo bash bench/realnode/uninstall.sh    # removes the node completely
bash bench/realnode/stop.sh --purge
```

Several real nodes on a private network, on one throwaway Linux machine
(each node has its own network namespace, containerd and kubelet; the
control plane cannot dial them, they reach the internet through NAT):

```sh
sudo bash bench/realnode/multinode.sh up 2    # instead of install.sh
bash bench/realnode/multinode-check.sh        # pods on every node, traffic between nodes, services, DNS, internet, logs and exec through the tunnel
sudo bash bench/realnode/multinode.sh down
```

## Conformance

The official Kubernetes conformance suite (v1.37.2, 462 tests, run with
hydrophone) runs in full on a nostromo cluster with two real nodes, engines
sleeping as usual. The latest full run, about two hours on a laptop: 460
passed; the 2 that did not were waiting for a test image the nodes could not
download from the registry in time, and both passed when run again. Every one
of the 462 tests has passed on nostromo.
