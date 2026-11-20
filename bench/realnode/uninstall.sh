#!/usr/bin/env bash
# Undoes install.sh. RUN WITH SUDO. Stops the node, removes its containers,
# binaries, configuration and data, and puts the system settings back.
# Safe to run more than once. It only removes what install.sh put there.
set -uo pipefail

[ "$(id -u)" = 0 ] || { echo "run it with sudo"; exit 1; }
MARK=/var/lib/nostromo-realnode
[ -d "$MARK" ] || { echo "install.sh has not been run on this machine (no $MARK): nothing to remove"; exit 0; }

echo "stopping the node..."
systemctl disable --now kubelet >/dev/null 2>&1
if [ -S /run/containerd/containerd.sock ]; then
  /usr/local/bin/kubeadm reset -f --cri-socket unix:///run/containerd/containerd.sock >/dev/null 2>&1
  # whatever containers are left
  for t in $(/usr/local/bin/ctr -n k8s.io tasks ls -q 2>/dev/null); do /usr/local/bin/ctr -n k8s.io tasks kill -s SIGKILL "$t" >/dev/null 2>&1; done
  sleep 2
  for c in $(/usr/local/bin/ctr -n k8s.io containers ls -q 2>/dev/null); do
    /usr/local/bin/ctr -n k8s.io tasks delete -f "$c" >/dev/null 2>&1
    /usr/local/bin/ctr -n k8s.io containers delete "$c" >/dev/null 2>&1
  done
fi
systemctl disable --now containerd >/dev/null 2>&1
pkill -9 -x containerd-shim-runc-v2 2>/dev/null

echo "unmounting what the node left mounted..."
for m in $(awk '$2 ~ "^/var/lib/kubelet|^/run/containerd|^/run/netns|^/var/lib/containerd" {print $2}' /proc/mounts | sort -r); do umount -l "$m" 2>/dev/null; done
for ns in $(ip netns list 2>/dev/null | awk '/^cni-/ {print $1}'); do ip netns delete "$ns" 2>/dev/null; done
ip link delete cni0 2>/dev/null

echo "removing files..."
rm -f /usr/local/bin/containerd /usr/local/bin/containerd-shim-runc-v2 /usr/local/bin/ctr /usr/local/bin/runc /usr/local/bin/kubelet /usr/local/bin/kubeadm
rm -f /etc/systemd/system/containerd.service /etc/systemd/system/kubelet.service
rm -rf /etc/systemd/system/kubelet.service.d
[ -e "$MARK/wrote-default-kubelet" ] && rm -f /etc/default/kubelet
rm -rf /etc/containerd /etc/cni /opt/cni /etc/kubernetes /var/lib/kubelet /var/lib/containerd /run/containerd /var/lib/cni /var/run/kubernetes
rmdir /opt/containerd 2>/dev/null
systemctl daemon-reload
systemctl reset-failed kubelet containerd >/dev/null 2>&1

# Rules kube-proxy left in the kernel go away with the next WSL restart
# (wsl --shutdown); removing them here would need tools this machine lacks.
[ -f "$MARK/ip_forward.before" ] && sysctl -q -w net.ipv4.ip_forward="$(cat "$MARK/ip_forward.before")"
rm -rf "$MARK"

echo "Done. The node is gone from this machine."
echo "Leftover packet rules from kube-proxy, if any, disappear at the next 'wsl --shutdown'."
