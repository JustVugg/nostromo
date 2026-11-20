#!/usr/bin/env bash
# Step 2 of the real-node test. RUN WITH SUDO. Turns this machine into a real
# Kubernetes worker node of the nostromo cluster "demo" created by start.sh.
#
# What it does, all of it undone by uninstall.sh:
#   - copies containerd, runc, kubelet, kubeadm into /usr/local/bin
#   - copies the standard network plugins into /opt/cni/bin
#   - writes /etc/containerd/config.toml, /etc/cni/net.d/10-nostromo.conflist,
#     /etc/default/kubelet and two systemd units (containerd, kubelet)
#   - turns on IPv4 forwarding and makes / a shared mount (until next reboot)
#   - runs the "kubeadm join" command printed by nostromo
# It installs no packages and downloads nothing itself: the binaries were
# fetched (and checksummed) beforehand into ~/nostromo-tools. Once the node
# runs, containerd pulls container images from registry.k8s.io and docker.io.
#
# It refuses to run if a container runtime or kubelet is already installed.
set -euo pipefail

[ "$(id -u)" = 0 ] || { echo "run it with sudo"; exit 1; }
# Where the binaries and the join command are. On the machine where start.sh
# ran they are in the home of the user calling sudo; on another machine, copy
# them over and point NOSTROMO_TOOLS and NOSTROMO_STATE at them.
if [ -n "${NOSTROMO_TOOLS:-}" ] && [ -n "${NOSTROMO_STATE:-}" ]; then
  TOOLS="$NOSTROMO_TOOLS"; STATE="$NOSTROMO_STATE"
else
  U="${SUDO_USER:-}"; [ -n "$U" ] && [ "$U" != root ] || { echo "run it with sudo from your normal user (or set NOSTROMO_TOOLS and NOSTROMO_STATE)"; exit 1; }
  H="$(getent passwd "$U" | cut -d: -f6)"
  TOOLS="$H/nostromo-tools"; STATE="$H/nostromo-realnode"
fi
NODE="$TOOLS/node"
MARK=/var/lib/nostromo-realnode

[ "$(ps -p 1 -o comm=)" = systemd ] || { echo "this machine does not run systemd"; exit 1; }
for f in "$NODE/containerd/bin/containerd" "$NODE/runc.amd64" "$NODE/kubelet" "$NODE/cni/bridge" "$TOOLS/k8s/kubeadm" "$STATE/join.cmd"; do
  [ -e "$f" ] || { echo "missing $f (run start.sh first)"; exit 1; }
done
for c in containerd dockerd kubelet; do
  if command -v "$c" >/dev/null 2>&1; then echo "$c is already installed here: not touching this machine"; exit 1; fi
done
for d in /etc/kubernetes /var/lib/kubelet /etc/cni/net.d /etc/containerd "$MARK"; do
  [ ! -e "$d" ] || { echo "$d already exists: not touching this machine (uninstall.sh removes a previous run)"; exit 1; }
done

# The join command is data written by another program: check its exact shape
# before running any of it as root.
JOIN="$(head -1 "$STATE/join.cmd")"
if ! [[ "$JOIN" =~ ^kubeadm\ join\ ([0-9.]+:[0-9]+)\ --token\ ([a-z0-9]{6}\.[a-z0-9]{16})\ --discovery-token-ca-cert-hash\ (sha256:[0-9a-f]{64})\ *$ ]]; then
  echo "unexpected join command in $STATE/join.cmd"; exit 1
fi
ENDPOINT="${BASH_REMATCH[1]}"; TOKEN="${BASH_REMATCH[2]}"; HASH="${BASH_REMATCH[3]}"

mkdir -p "$MARK"
cat /proc/sys/net/ipv4/ip_forward >"$MARK/ip_forward.before"

echo "installing binaries..."
install -m 0755 "$NODE/containerd/bin/containerd" "$NODE/containerd/bin/containerd-shim-runc-v2" "$NODE/containerd/bin/ctr" /usr/local/bin/
install -m 0755 "$NODE/runc.amd64" /usr/local/bin/runc
install -m 0755 "$NODE/kubelet" "$TOOLS/k8s/kubeadm" /usr/local/bin/
mkdir -p /opt/cni/bin
for p in bridge host-local loopback portmap; do install -m 0755 "$NODE/cni/$p" /opt/cni/bin/; done

echo "writing configuration..."
mkdir -p /etc/containerd /etc/cni/net.d /etc/systemd/system/kubelet.service.d
cat >/etc/containerd/config.toml <<'EOF'
version = 3
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc]
  runtime_type = 'io.containerd.runc.v2'
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc.options]
  SystemdCgroup = true
EOF
# The simplest pod network there is: one bridge on this one node.
cat >/etc/cni/net.d/10-nostromo.conflist <<'EOF'
{
  "cniVersion": "1.0.0",
  "name": "nostromo-realnode",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "cni0",
      "isGateway": true,
      "hairpinMode": true,
      "ipMasq": false,
      "ipam": {
        "type": "host-local",
        "ranges": [[{"subnet": "10.244.0.0/24"}]],
        "routes": [{"dst": "0.0.0.0/0"}]
      }
    },
    {"type": "portmap", "capabilities": {"portMappings": true}}
  ]
}
EOF
cat >/etc/systemd/system/containerd.service <<'EOF'
[Unit]
Description=containerd container runtime (nostromo real-node test)
After=network.target

[Service]
ExecStart=/usr/local/bin/containerd
Type=notify
Delegate=yes
KillMode=process
Restart=always
RestartSec=5
LimitNPROC=infinity
LimitCORE=infinity
LimitNOFILE=infinity
TasksMax=infinity
OOMScoreAdjust=-999

[Install]
WantedBy=multi-user.target
EOF
cat >/etc/systemd/system/kubelet.service <<'EOF'
[Unit]
Description=kubelet (nostromo real-node test)
After=containerd.service
Wants=containerd.service

[Service]
ExecStart=/usr/local/bin/kubelet
Restart=always
StartLimitInterval=0
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
# The standard drop-in kubeadm expects.
cat >/etc/systemd/system/kubelet.service.d/10-kubeadm.conf <<'EOF'
[Service]
Environment="KUBELET_KUBECONFIG_ARGS=--bootstrap-kubeconfig=/etc/kubernetes/bootstrap-kubelet.conf --kubeconfig=/etc/kubernetes/kubelet.conf"
Environment="KUBELET_CONFIG_ARGS=--config=/var/lib/kubelet/config.yaml"
EnvironmentFile=-/var/lib/kubelet/kubeadm-flags.env
EnvironmentFile=-/etc/default/kubelet
ExecStart=
ExecStart=/usr/local/bin/kubelet $KUBELET_KUBECONFIG_ARGS $KUBELET_CONFIG_ARGS $KUBELET_KUBEADM_ARGS $KUBELET_EXTRA_ARGS
EOF
# WSL keeps swap on; a test node can live with it.
[ -e /etc/default/kubelet ] || { echo 'KUBELET_EXTRA_ARGS=--fail-swap-on=false' >/etc/default/kubelet; touch "$MARK/wrote-default-kubelet"; }

echo "preparing the system..."
sysctl -q -w net.ipv4.ip_forward=1
mount --make-rshared /            # kubelet needs it; WSL mounts / as private
modprobe br_netfilter 2>/dev/null || true

systemctl daemon-reload
systemctl enable --now containerd
for _ in $(seq 1 50); do [ -S /run/containerd/containerd.sock ] && break; sleep 0.2; done
[ -S /run/containerd/containerd.sock ] || { echo "containerd did not start: journalctl -u containerd"; exit 1; }
systemctl enable kubelet >/dev/null 2>&1

echo "joining the cluster at $ENDPOINT..."
/usr/local/bin/kubeadm join "$ENDPOINT" --token "$TOKEN" --discovery-token-ca-cert-hash "$HASH" \
  --node-name nostromo-node1 --cri-socket unix:///run/containerd/containerd.sock \
  --ignore-preflight-errors=all

echo
echo "Done. This machine is now a worker node of the nostromo cluster 'demo'."
echo "Check it (no sudo):   bash $(dirname "$(readlink -f "$0")")/check.sh"
echo "Remove everything:    sudo bash $(dirname "$(readlink -f "$0")")/uninstall.sh"
