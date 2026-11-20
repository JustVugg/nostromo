#!/usr/bin/env bash
# Checks the cluster "demo" with the nodes started by multinode.sh (no root
# needed here): nodes on a private network that cannot be dialled from the
# control plane's side, pods on every node, traffic between nodes, services,
# DNS, the way out to the internet, and logs/exec through the tunnel.
set -u
TOOLS="${NOSTROMO_TOOLS:-$HOME/nostromo-tools}"
STATE="$HOME/nostromo-realnode"
KC="$TOOLS/k8s/kubectl --kubeconfig $STATE/demo.kubeconfig --request-timeout=60s"
k() { $KC "$@"; }
nap() { read -r -t "$1" <> <(:) || true; }
FAILED=0
check() { if [ "$1" = 0 ]; then echo "  OK: $2"; else echo "  FAILED: $2"; FAILED=$((FAILED+1)); fi; }
until_ok() { local n=$1; shift; for _ in $(seq 1 "$n"); do "$@" >/dev/null 2>&1 && return 0; nap 2; done; return 1; }
count_running() { [ "$(k -n "$1" get pods -l "$2" --no-headers 2>/dev/null | grep -c ' Running ')" = "$3" ]; }

WANT="${1:-2}"   # how many nodes multinode.sh was asked for
for _ in $(seq 1 60); do [ "$(k get nodes --no-headers 2>/dev/null | grep -c ' Ready ')" -ge "$WANT" ] && break; nap 2; done
NODES="$(k get nodes --no-headers 2>/dev/null | grep -c ' Ready ')"
echo "nodes"
[ "$NODES" -ge "$WANT" ] && [ "$NODES" -ge 2 ]; check $? "$NODES real nodes are Ready"
k get nodes -o wide 2>&1 | cut -c1-100 | sed 's/^/  /'
BLOCKED=0
for ip in $(k get nodes -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}{end}'); do
  timeout 3 bash -c "exec 3<>/dev/tcp/$ip/10250" 2>/dev/null && BLOCKED=1
done
check $BLOCKED "the nodes' kubelets cannot be dialled from the control plane's side"

echo "system pods (images are pulled on every node: can take a few minutes)"
until_ok 150 count_running kube-system k8s-app=kube-proxy "$NODES"; check $? "kube-proxy runs on every node"
until_ok 150 count_running kube-system k8s-app=konnectivity-agent "$NODES"; check $? "the tunnel agent runs on every node"
until_ok 150 count_running kube-system k8s-app=kube-dns 2; check $? "CoreDNS is running"

echo "an application on every node"
k delete daemonset web --ignore-not-found >/dev/null 2>&1; k delete service web --ignore-not-found >/dev/null 2>&1
# pods of a previous run must be gone, or they would be mistaken for the new ones
until_ok 60 count_running default app=web 0
k wait --for=delete pod -l app=web --timeout=60s >/dev/null 2>&1
k apply -f - >/dev/null <<'EOF'
apiVersion: apps/v1
kind: DaemonSet
metadata: {name: web}
spec:
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
      - name: web
        image: busybox:1.36
        command: ["sh", "-c", "mkdir -p /www && echo \"served by $NODE_NAME\" > /www/index.html && echo \"started on $NODE_NAME\" && httpd -f -p 8080 -h /www"]
        env: [{name: NODE_NAME, valueFrom: {fieldRef: {fieldPath: spec.nodeName}}}]
---
apiVersion: v1
kind: Service
metadata: {name: web}
spec:
  selector: {app: web}
  ports: [{port: 80, targetPort: 8080}]
EOF
until_ok 150 count_running default app=web "$NODES"; check $? "one container runs on each node"
PODS="$(k get pods -l app=web -o jsonpath='{range .items[*]}{.metadata.name} {.spec.nodeName} {.status.podIP}{"\n"}{end}')"
LOGS=0; EXEC=0
while read -r pod node ip; do
  [ -n "$pod" ] || continue
  [ "$(k logs "$pod" 2>/dev/null | head -1)" = "started on $node" ] || LOGS=1
  [ "$(k exec "$pod" -- hostname 2>/dev/null)" = "$pod" ] || EXEC=1
done <<<"$PODS"
check $LOGS "kubectl logs works for the pod on every node (only the tunnel can reach them)"
check $EXEC "kubectl exec works for the pod on every node"

echo "network"
FIRST="$(echo "$PODS" | head -1 | cut -d' ' -f1)"
CROSS=0
while read -r pod node ip; do
  [ -n "$pod" ] || continue
  [ "$(k exec "$FIRST" -- wget -q -T 5 -O- "http://$ip:8080/" 2>/dev/null)" = "served by $node" ] || CROSS=1
done <<<"$PODS"
check $CROSS "a pod reaches the pods on every node, its own and the others"
SEEN="$(for _ in $(seq 1 12); do k exec "$FIRST" -- wget -q -T 5 -O- http://web.default.svc.cluster.local/ 2>/dev/null; echo; done | sort -u | grep -c 'served by')"
[ "$SEEN" = "$NODES" ]; check $? "a service, called by name, spreads requests over the pods of all nodes (answers from $SEEN nodes)"
k exec "$FIRST" -- nslookup registry.k8s.io >/dev/null 2>&1; check $? "pods resolve names on the internet"
k exec "$FIRST" -- sh -c 'nc -w 5 registry.k8s.io 443 </dev/null' >/dev/null 2>&1; check $? "pods open connections to the internet"

echo
"$STATE/nostromo" list 2>/dev/null | sed 's/^/  /'
echo
if [ "$FAILED" = 0 ]; then echo "ALL OK"; else echo "$FAILED checks failed"; fi
exit "$FAILED"
