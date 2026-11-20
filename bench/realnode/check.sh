#!/usr/bin/env bash
# Step 3 of the real-node test (no root needed). Checks that the cluster
# "demo" really works with the real node joined by install.sh:
# a container runs, its logs and a command inside it come back through the
# tunnel, and from inside the pod the cluster's own API and DNS answer.
set -u
TOOLS="${NOSTROMO_TOOLS:-$HOME/nostromo-tools}"
STATE="$HOME/nostromo-realnode"
k() { "$TOOLS/k8s/kubectl" --kubeconfig "$STATE/demo.kubeconfig" --request-timeout=60s "$@"; }
nap() { read -r -t "$1" <> <(:) || true; }
FAILED=0
check() { if [ "$1" = 0 ]; then echo "  OK: $2"; else echo "  FAILED: $2"; FAILED=$((FAILED+1)); fi; }
# until SECONDS command...: retry until the command succeeds
until_ok() { local n=$1; shift; for _ in $(seq 1 "$n"); do "$@" >/dev/null 2>&1 && return 0; nap 2; done; return 1; }
running() { [ "$(k -n "$1" get pods -l "$2" -o jsonpath='{.items[0].status.phase}' 2>/dev/null)" = Running ]; }

echo "the node"
until_ok 90 sh -c "\"$TOOLS/k8s/kubectl\" --kubeconfig \"$STATE/demo.kubeconfig\" get node nostromo-node1 -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}' | grep -q True"
check $? "a real kubelet joined and the node is Ready"
k get nodes -o wide 2>&1 | sed 's/^/  /' | cut -c1-150

echo "system pods (images are pulled from the internet: can take a few minutes)"
until_ok 150 running kube-system k8s-app=kube-proxy; check $? "kube-proxy is running"
until_ok 150 running kube-system k8s-app=konnectivity-agent; check $? "the tunnel agent is running as a pod"
until_ok 150 running kube-system k8s-app=kube-dns; check $? "CoreDNS is running"

echo "an application"
k delete pod hello --ignore-not-found --wait=true >/dev/null 2>&1
k run hello --image=busybox:1.36 --restart=Never -- sh -c 'echo hello from a real pod; while true; do sleep 60; done' >/dev/null; check $? "pod created"
until_ok 150 sh -c "[ \"\$(\"$TOOLS/k8s/kubectl\" --kubeconfig \"$STATE/demo.kubeconfig\" get pod hello -o jsonpath='{.status.phase}')\" = Running ]"
check $? "the container is running on the node"
OUT="$(k logs hello 2>&1 | head -1)"; [ "$OUT" = "hello from a real pod" ]; check $? "kubectl logs returns its output through the tunnel (got: $(echo "$OUT" | cut -c1-60))"
OUT="$(k exec hello -- hostname 2>&1 | head -1)"; [ "$OUT" = hello ]; check $? "kubectl exec runs a command inside it (got: $(echo "$OUT" | cut -c1-60))"

echo "from inside the pod"
# A real Kubernetes client inside a pod, using what every pod is given: the
# service address, the cluster's CA and its own token. (busybox's wget cannot
# be used for this: its TLS is too limited, and its failure kills the pod.)
k delete pod incluster --ignore-not-found --wait=true >/dev/null 2>&1
k run incluster --image=registry.k8s.io/kubectl:v1.37.2 --restart=Never -- get --raw /version >/dev/null
until_ok 90 sh -c "\"$TOOLS/k8s/kubectl\" --kubeconfig \"$STATE/demo.kubeconfig\" get pod incluster -o jsonpath='{.status.phase}' | grep -Eq 'Succeeded|Failed'"
OUT="$(k logs incluster 2>&1)"
echo "$OUT" | grep -q gitVersion; check $? "a pod reaches the cluster's API through the 'kubernetes' service, verifying its certificate ($(echo "$OUT" | tr -d '\n ' | cut -c1-40))"
k delete pod incluster --wait=false >/dev/null 2>&1
OUT="$(k exec hello -- nslookup kubernetes.default.svc.cluster.local 2>&1)"
echo "$OUT" | grep -q "10.96.0.1"; check $? "the cluster's DNS resolves service names"

echo
k get pods -A -o wide 2>&1 | sed 's/^/  /' | cut -c1-130
echo
NOSTROMO_DIR="$STATE/data" "$STATE/nostromo" list 2>/dev/null | sed 's/^/  /'
echo
if [ "$FAILED" = 0 ]; then echo "ALL OK"; else echo "$FAILED checks failed"; fi
exit "$FAILED"
