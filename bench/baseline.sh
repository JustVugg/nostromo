#!/usr/bin/env bash
# Baseline: how much memory ONE classic, always-on Kubernetes control plane
# uses when idle (no nodes, no load), and how long its apiserver takes to
# start. This is the number nostromo is compared against.
# Everything runs locally on 127.0.0.1 and is stopped at the end.
#
# Usage: baseline.sh [idle_seconds]   (default 60)
set -u
TOOLS="${NOSTROMO_TOOLS:-$HOME/nostromo-tools}"
K8S="$TOOLS/k8s"
ETCD="$TOOLS/etcd/etcd"
IDLE="${1:-60}"
D="$(mktemp -d "${TMPDIR:-/tmp}/nostromo-baseline.XXXXXX")"
PIDS=()

cleanup() {
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  sleep 2
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done
}
trap cleanup EXIT

now_ms() { date +%s%3N; }

wait_ready() { # wait until the apiserver answers "ok" on /readyz
  for _ in $(seq 1 600); do
    [ "$(curl -m 2 -sk https://127.0.0.1:16443/readyz 2>/dev/null)" = "ok" ] && return 0
    sleep 0.1
  done
  return 1
}

rss_mb() { awk '/^VmRSS/ {printf "%.0f", $2/1024}' "/proc/$1/status" 2>/dev/null; }

start_apiserver() {
  "$K8S/kube-apiserver" \
    --etcd-servers=http://127.0.0.1:12379 \
    --bind-address=127.0.0.1 --secure-port=16443 \
    --cert-dir="$D/certs" \
    --service-account-key-file="$D/sa.pub" \
    --service-account-signing-key-file="$D/sa.key" \
    --service-account-issuer=https://kubernetes.default.svc \
    --token-auth-file="$D/tokens.csv" \
    --authorization-mode=RBAC \
    --service-cluster-ip-range=10.96.0.0/12 \
    >"$D/apiserver.log" 2>&1 &
  PID_API=$!
}

# --- setup: keys, token, kubeconfig ---
openssl genrsa -out "$D/sa.key" 2048 2>/dev/null
openssl rsa -in "$D/sa.key" -pubout -out "$D/sa.pub" 2>/dev/null
TOKEN="$(openssl rand -hex 16)"
echo "$TOKEN,admin,admin,system:masters" >"$D/tokens.csv"
cat >"$D/kubeconfig" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: t
  cluster: {server: "https://127.0.0.1:16443", insecure-skip-tls-verify: true}
users:
- name: admin
  user: {token: "$TOKEN"}
contexts:
- name: t
  context: {cluster: t, user: admin}
current-context: t
EOF

# --- etcd ---
"$ETCD" --data-dir="$D/etcd" \
  --listen-client-urls=http://127.0.0.1:12379 --advertise-client-urls=http://127.0.0.1:12379 \
  --listen-peer-urls=http://127.0.0.1:12380 --initial-advertise-peer-urls=http://127.0.0.1:12380 \
  --initial-cluster=default=http://127.0.0.1:12380 \
  >"$D/etcd.log" 2>&1 &
PID_ETCD=$!; PIDS+=("$PID_ETCD")
sleep 2

# --- first boot (empty datastore) ---
T0=$(now_ms); start_apiserver; PIDS+=("$PID_API")
wait_ready || { echo "ERROR: apiserver not ready"; tail -20 "$D/apiserver.log"; exit 1; }
T_FIRST=$(( $(now_ms) - T0 ))

"$K8S/kube-controller-manager" --kubeconfig="$D/kubeconfig" \
  --service-account-private-key-file="$D/sa.key" --use-service-account-credentials \
  --leader-elect=false --bind-address=127.0.0.1 \
  >"$D/cm.log" 2>&1 &
PID_CM=$!; PIDS+=("$PID_CM")
"$K8S/kube-scheduler" --kubeconfig="$D/kubeconfig" \
  --leader-elect=false --bind-address=127.0.0.1 \
  >"$D/sched.log" 2>&1 &
PID_SCHED=$!; PIDS+=("$PID_SCHED")

echo "control plane up, leaving it idle for ${IDLE}s..."
sleep "$IDLE"
for n in ETCD API CM SCHED; do v="PID_$n"; kill -0 "${!v}" 2>/dev/null || { echo "ERROR: $n died"; tail -15 "$D"/*.log; exit 1; }; done

M_ETCD=$(rss_mb $PID_ETCD); M_API=$(rss_mb $PID_API); M_CM=$(rss_mb $PID_CM); M_SCHED=$(rss_mb $PID_SCHED)
NS=$("$K8S/kubectl" --kubeconfig="$D/kubeconfig" --request-timeout=5s get ns --no-headers 2>/dev/null | wc -l)

# --- restart: stop the apiserver and start it again (state already stored) ---
kill "$PID_API"; wait "$PID_API" 2>/dev/null
T0=$(now_ms); start_apiserver; PIDS+=("$PID_API")
wait_ready || { echo "ERROR: apiserver not ready on restart"; exit 1; }
T_RESTART=$(( $(now_ms) - T0 ))
T0=$(now_ms)
"$K8S/kubectl" --kubeconfig="$D/kubeconfig" --request-timeout=5s get ns >/dev/null 2>&1
T_REQUEST=$(( $(now_ms) - T0 ))

echo
echo "=== idle memory (MB, RSS) - empty cluster, $NS namespaces ==="
printf "%-26s %5s\n" "kube-apiserver" "$M_API" "kube-controller-manager" "$M_CM" "kube-scheduler" "$M_SCHED" "etcd" "$M_ETCD"
printf "%-26s %5s\n" "TOTAL control plane" "$((M_API+M_CM+M_SCHED))" "TOTAL with etcd" "$((M_API+M_CM+M_SCHED+M_ETCD))"
echo
echo "=== times (ms) ==="
printf "%-34s %6s\n" "apiserver first boot" "$T_FIRST" "apiserver restart (from stopped)" "$T_RESTART" "first request after restart" "$T_REQUEST"
echo
echo "etcd data size: $(du -sh "$D/etcd" | cut -f1)   logs in: $D"
