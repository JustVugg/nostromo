#!/usr/bin/env bash
# Worker nodes: what a node needs in order to join and work.
#  1. the cluster is reachable on its dedicated port by plain IP address
#  2. the cluster advertises that address to its pods (the "kubernetes" service)
#  3. "nostromo join" prepares the cluster with kubeadm and returns a join command
#  4. a node joining like a kubelet (token -> certificate -> own identity) is accepted
#  5. the standard add-ons (kube-proxy, CoreDNS) are installed
# No real kubelet is started (that needs root): bench/fakenode follows the same
# steps a kubelet does towards the control plane. Takes about 1 minute.
source "$(dirname "$0")/lib.sh"

IP="$(node_ip)"
[ -n "$IP" ] || { echo "no non-loopback IPv4 address on this machine: cannot test nodes"; exit 1; }
echo "node address: $IP"

up 1 -listen "$IP:6443" -address "$IP" -node-address "$IP" -idle 30s -quiet 15s
"$N" create alpha >/dev/null
"$N" kubeconfig alpha >"$D/alpha.kc"
PORT="$(cluster alpha port)"
T="$D/data/tenants/alpha"

echo "dedicated port"
k alpha get namespaces >/dev/null; check $? "users reach the cluster through the shared port"
[ "$("$K" --server "https://$IP:$PORT" --certificate-authority "$T/ca.crt" --client-certificate "$T/admin.crt" --client-key "$T/admin.key" --request-timeout=30s get namespace default -o name 2>/dev/null)" = namespace/default ]
check $? "the cluster answers on its own port $PORT by plain IP (no name in the TLS hello)"
EP="$(k alpha get endpointslice kubernetes -o jsonpath='{.endpoints[0].addresses[0]}:{.ports[0].port}' 2>/dev/null)"
[ "$EP" = "$IP:$PORT" ]; check $? "the cluster advertises $IP:$PORT to its pods (found: ${EP:-nothing})"

echo "join"
JOIN="$("$N" join alpha 2>"$D/join.err")"; check $? "nostromo join returns a command"
echo "  $JOIN" | cut -c1-110
[ -s "$D/join.err" ] && sed 's/^/  /' "$D/join.err"
TOKEN="$(echo "$JOIN" | sed -n 's/.*--token \([a-z0-9.]*\).*/\1/p')"
HASH="$(echo "$JOIN" | sed -n 's/.*sha256:\([0-9a-f]*\).*/\1/p')"
echo "$JOIN" | grep -q "kubeadm join $IP:$PORT "; check $? "the command points nodes at $IP:$PORT"
REAL="$(openssl x509 -in "$T/ca.crt" -pubkey -noout | openssl pkey -pubin -outform DER 2>/dev/null | sha256sum | cut -d' ' -f1)"
[ -n "$HASH" ] && [ "$HASH" = "$REAL" ]; check $? "the fingerprint in the command is the cluster's own authority"
OK=1
for _ in $(seq 1 30); do # signed by the engines, a moment after the token is created
  k alpha -n kube-public get configmap cluster-info -o jsonpath='{.data}' 2>/dev/null | grep -q "jws-kubeconfig-" && { OK=0; break; }; nap 1
done
check $OK "the cluster signed its public info for new nodes"
k alpha -n kube-system get configmap kubeadm-config kubelet-config >/dev/null 2>&1; check $? "cluster and kubelet settings are published for new nodes"
k alpha -n kube-system get daemonset kube-proxy >/dev/null 2>&1; check $? "kube-proxy add-on installed"
k alpha -n kube-system get deployment coredns >/dev/null 2>&1; check $? "CoreDNS add-on installed"
k alpha -n kube-system get configmap kube-proxy -o jsonpath='{.data.kubeconfig\.conf}' 2>/dev/null | grep -q "server: https://$IP:$PORT"; check $? "kube-proxy is told to reach the API at $IP:$PORT"

echo "a node joins like a kubelet"
"$D/fakenode" -token "$TOKEN" -ca "$T/ca.crt" -server "$IP:$PORT" -name worker1 -status 20s >"$D/worker1.log" 2>&1 & EXTRA_PIDS=$!
OK=1
for _ in $(seq 1 60); do
  [ "$(k alpha get node worker1 -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = True ] && { OK=0; break; }; nap 1
done
check $OK "worker1 got its certificate from the cluster and registered as Ready"
grep -q "received its certificate" "$D/worker1.log"; check $? "the certificate was issued through the bootstrap token (no admin credentials on the node)"
SIGNER="$(k alpha get csr -o jsonpath='{.items[0].spec.username}' 2>/dev/null)"
case "$SIGNER" in system:bootstrap:*) true;; *) false;; esac; check $? "the request came from the token identity ($SIGNER)"
not "$D/fakenode" -token "aaaaaa.bbbbbbbbbbbbbbbb" -ca "$T/ca.crt" -server "$IP:$PORT" -name intruder; check $? "a wrong token is refused"
k alpha get nodes 2>&1 | sed 's/^/  /'

echo "pods are scheduled on the joined node"
k alpha create deployment web --image=nginx --replicas=2 >/dev/null
OK=1
for _ in $(seq 1 40); do
  [ "$(k alpha get pods -l app=web -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | grep -c worker1)" = 2 ] && { OK=0; break; }; nap 1
done
check $OK "2 pods assigned to worker1"

verdict
