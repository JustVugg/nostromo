#!/usr/bin/env bash
# Several real worker nodes on one Linux machine, on a network of their own.
# RUN AS ROOT, on a machine you can throw away.
#
#   multinode.sh up [N]     start N nodes (default 2) and join them to the cluster
#   multinode.sh down       stop them and remove everything
#
# Each node gets its own network namespace (so its own address, ports and
# packet rules), its own containerd and its own kubelet. The nodes sit on a
# private network, 172.30.0.0/24, behind this machine:
#   - they reach the outside (nostromo, the internet) through address translation
#   - nothing outside is allowed to open a connection to their kubelets
# which is how customer nodes usually look from a provider's side.
#
# Needs: iptables, the binaries under $NOSTROMO_TOOLS/node, and in
# $NOSTROMO_STATE the files join.cmd (from "nostromo join") and ca.crt (the
# cluster's CA). Nodes join with the same token mechanism kubeadm uses.
set -uo pipefail
[ "$(id -u)" = 0 ] || { echo "run as root"; exit 1; }
TOOLS="${NOSTROMO_TOOLS:-/opt/nostromo-tools}"
STATE="${NOSTROMO_STATE:-/opt/nostromo-realnode}"
NODE="$TOOLS/node"
ETC=/etc/nostromo-nodes
NET=172.30.0
BR=nbr0
# Largest packet on the node and pod networks. Lower than the usual 1500
# because the path from here to the internet may not carry full-size packets
# (it does not under WSL), and then large downloads hang.
MTU="${NOSTROMO_MTU:-1400}"
OUT_IF="$(ip -4 route show default | awk '{print $5; exit}')"

name() { echo "nostromo-node$1"; }

rules() { # rules add|del
  local op=-A chk
  [ "$1" = del ] && op=-D
  for r in \
    "nat POSTROUTING -s $NET.0/24 -o $OUT_IF -j MASQUERADE" \
    "nat POSTROUTING -s 10.244.0.0/16 -o $OUT_IF -j MASQUERADE" \
    "filter OUTPUT -d $NET.0/24 -p tcp --dport 10250 -j REJECT" \
    "filter FORWARD ! -s $NET.0/24 -d $NET.0/24 -p tcp --dport 10250 -j REJECT"; do
    set -- $r; local table=$1 chain=$2; shift 2
    if iptables -t "$table" -C "$chain" "$@" 2>/dev/null; then chk=yes; else chk=no; fi
    if [ "$op" = -A ] && [ "$chk" = no ]; then iptables -t "$table" -A "$chain" "$@"; fi
    if [ "$op" = -D ] && [ "$chk" = yes ]; then iptables -t "$table" -D "$chain" "$@"; fi
  done
}

down() {
  echo "stopping nodes..."
  for u in $(systemctl list-units --all --no-legend 'nostromo-node*' 2>/dev/null | awk '{print $1}'); do systemctl disable --now "$u" >/dev/null 2>&1; done
  for d in /run/nostromo-node*; do
    [ -S "$d/containerd/containerd.sock" ] || continue
    for t in $("$NODE/containerd/bin/ctr" -a "$d/containerd/containerd.sock" -n k8s.io tasks ls -q 2>/dev/null); do
      "$NODE/containerd/bin/ctr" -a "$d/containerd/containerd.sock" -n k8s.io tasks kill -s SIGKILL "$t" >/dev/null 2>&1
    done
  done
  pkill -9 -f 'containerd-shim-runc-v2.*nostromo-node' 2>/dev/null
  sleep 2
  for m in $(awk '$2 ~ "^/var/lib/nostromo-node|^/run/nostromo-node" {print $2}' /proc/mounts | sort -r); do umount -l "$m" 2>/dev/null; done
  for ns in $(ip netns list 2>/dev/null | awk '/^nostromo-node|^cni-/ {print $1}'); do ip netns delete "$ns" 2>/dev/null; done
  ip link delete "$BR" 2>/dev/null
  for l in $(ip -o link show | awk -F': ' '/nnode[0-9]+@/ {print $2}' | cut -d@ -f1); do ip link delete "$l" 2>/dev/null; done
  rules del
  rm -f /etc/systemd/system/nostromo-node*.service
  systemctl daemon-reload
  rm -rf "$ETC" /var/lib/nostromo-node* /run/nostromo-node* /var/log/nostromo-node*
  for c in /sys/fs/cgroup/nostromo-node*; do [ -d "$c" ] && find "$c" -depth -type d -exec rmdir {} \; 2>/dev/null; done
  [ -f /var/lib/nostromo-multinode.ip_forward ] && sysctl -q -w net.ipv4.ip_forward="$(cat /var/lib/nostromo-multinode.ip_forward)" && rm -f /var/lib/nostromo-multinode.ip_forward
  echo "done."
}

up() {
  local count="${1:-2}"
  for f in "$NODE/containerd/bin/containerd" "$NODE/runc.amd64" "$NODE/kubelet" "$NODE/cni/bridge" "$STATE/join.cmd" "$STATE/ca.crt"; do
    [ -e "$f" ] || { echo "missing $f"; exit 1; }
  done
  command -v iptables >/dev/null || { echo "iptables is needed"; exit 1; }
  [ -e "$ETC" ] && { echo "nodes already set up: run '$0 down' first"; exit 1; }

  # The join command is data from another program: check its shape, and that
  # the CA we were given is the one it names.
  local join endpoint token hash
  join="$(head -1 "$STATE/join.cmd")"
  if ! [[ "$join" =~ ^kubeadm\ join\ ([0-9.]+:[0-9]+)\ --token\ ([a-z0-9]{6}\.[a-z0-9]{16})\ --discovery-token-ca-cert-hash\ sha256:([0-9a-f]{64})\ *$ ]]; then
    echo "unexpected join command"; exit 1
  fi
  endpoint="${BASH_REMATCH[1]}"; token="${BASH_REMATCH[2]}"; hash="${BASH_REMATCH[3]}"
  [ "$(openssl x509 -in "$STATE/ca.crt" -pubkey -noout | openssl pkey -pubin -outform DER 2>/dev/null | sha256sum | cut -d' ' -f1)" = "$hash" ] \
    || { echo "ca.crt is not the authority named in the join command"; exit 1; }

  mkdir -p "$ETC" /opt/cni/bin /usr/local/bin
  install -m 0755 "$NODE/runc.amd64" /usr/local/bin/runc
  install -m 0755 "$NODE/containerd/bin/containerd-shim-runc-v2" /usr/local/bin/
  for p in bridge host-local loopback portmap; do install -m 0755 "$NODE/cni/$p" /opt/cni/bin/; done
  cp "$STATE/ca.crt" "$ETC/ca.crt"
  # Pods get public resolvers: the machine's own resolver only answers locally.
  printf 'nameserver 1.1.1.1\nnameserver 8.8.8.8\n' >"$ETC/resolv.conf"

  cat /proc/sys/net/ipv4/ip_forward >/var/lib/nostromo-multinode.ip_forward
  sysctl -q -w net.ipv4.ip_forward=1
  mount --make-rshared /
  ip link add "$BR" mtu "$MTU" type bridge && ip addr add "$NET.1/24" dev "$BR" && ip link set "$BR" up
  rules add

  for i in $(seq 1 "$count"); do
    local n; n="$(name "$i")"
    local ip="$NET.1$i" e="$ETC/$n" lib="/var/lib/$n" run="/run/$n"
    echo "starting $n ($ip)..."
    mkdir -p "$e/cni" "$lib/kubelet" "$run/containerd" "/var/log/$n/pods"

    # network: a namespace wired to the bridge
    ip netns add "$n"
    ip link add "nnode$i" mtu "$MTU" type veth peer name eth0 mtu "$MTU" netns "$n"
    ip link set "nnode$i" master "$BR" up
    ip -n "$n" addr add "$ip/24" dev eth0
    ip -n "$n" link set lo up
    ip -n "$n" link set eth0 up
    ip -n "$n" route add default via "$NET.1"
    ip netns exec "$n" sysctl -q -w net.ipv4.ip_forward=1
    ip route replace "10.244.$i.0/24" via "$ip"     # pods of this node

    # cgroups: each kubelet gets its own corner
    mkdir -p "/sys/fs/cgroup/$n"
    echo "+cpuset +cpu +io +memory +hugetlb +pids" >"/sys/fs/cgroup/$n/cgroup.subtree_control" 2>/dev/null || true

    cat >"$e/containerd.toml" <<EOF
version = 3
root = "$lib/containerd"
state = "$run/containerd"
[grpc]
  address = "$run/containerd/containerd.sock"
[debug]
  address = "$run/containerd/debug.sock"
[plugins.'io.containerd.cri.v1.runtime'.cni]
  bin_dirs = ['/opt/cni/bin']
  conf_dir = '$e/cni'
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc]
  runtime_type = 'io.containerd.runc.v2'
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc.options]
  SystemdCgroup = false
EOF
    cat >"$e/cni/10-pods.conflist" <<EOF
{
  "cniVersion": "1.0.0",
  "name": "$n",
  "plugins": [
    {
      "type": "bridge", "bridge": "cni0", "isGateway": true, "ipMasq": false, "mtu": $MTU, "hairpinMode": true,
      "ipam": {"type": "host-local", "dataDir": "$lib/cni",
               "ranges": [[{"subnet": "10.244.$i.0/24"}]], "routes": [{"dst": "0.0.0.0/0"}]}
    },
    {"type": "portmap", "capabilities": {"portMappings": true}}
  ]
}
EOF
    cat >"$e/kubelet.yaml" <<EOF
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
authentication:
  anonymous: {enabled: false}
  webhook: {enabled: true}
  x509: {clientCAFile: $ETC/ca.crt}
authorization: {mode: Webhook}
cgroupDriver: cgroupfs
cgroupRoot: /$n
clusterDNS: [10.96.0.10]
clusterDomain: cluster.local
containerRuntimeEndpoint: unix://$run/containerd/containerd.sock
failSwapOn: false
podLogsDir: /var/log/$n/pods
resolvConf: $ETC/resolv.conf
rotateCertificates: true
EOF
    # what kubeadm writes on a node: the token to ask for a certificate with
    cat >"$e/bootstrap.kubeconfig" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: c
  cluster: {server: "https://$endpoint", certificate-authority: $ETC/ca.crt}
users:
- name: u
  user: {token: "$token"}
contexts:
- name: c
  context: {cluster: c, user: u}
current-context: c
EOF
    chmod 600 "$e/bootstrap.kubeconfig"
    cat >"/etc/systemd/system/$n-containerd.service" <<EOF
[Unit]
Description=containerd of $n (nostromo multi-node test)
[Service]
# nsenter, not NetworkNamespacePath=: that option also gives the service its
# own view of the mounts, and containers would not see the kubelet's volumes
ExecStart=/usr/bin/nsenter --net=/run/netns/$n -- $NODE/containerd/bin/containerd --config $e/containerd.toml
Type=notify
Delegate=yes
KillMode=process
Restart=always
RestartSec=5
LimitNOFILE=infinity
TasksMax=infinity
EOF
    cat >"/etc/systemd/system/$n-kubelet.service" <<EOF
[Unit]
Description=kubelet of $n (nostromo multi-node test)
After=$n-containerd.service
Wants=$n-containerd.service
[Service]
ExecStart=/usr/bin/nsenter --net=/run/netns/$n -- $NODE/kubelet --config=$e/kubelet.yaml --bootstrap-kubeconfig=$e/bootstrap.kubeconfig --kubeconfig=$e/kubelet.kubeconfig \\
  --root-dir=$lib/kubelet --cert-dir=$lib/kubelet/pki --hostname-override=$n --node-ip=$ip
Restart=always
RestartSec=5
EOF
  done
  systemctl daemon-reload
  for i in $(seq 1 "$count"); do systemctl start "$(name "$i")-containerd" "$(name "$i")-kubelet"; done
  sleep 3
  for i in $(seq 1 "$count"); do
    printf '%s: containerd %s, kubelet %s\n' "$(name "$i")" "$(systemctl is-active "$(name "$i")-containerd")" "$(systemctl is-active "$(name "$i")-kubelet")"
  done
}

case "${1:-}" in
  up) up "${2:-2}" ;;
  down) down ;;
  *) echo "usage: $0 up [N] | down"; exit 2 ;;
esac
