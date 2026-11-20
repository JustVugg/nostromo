#!/usr/bin/env bash
# The tunnel with several instances: two copies of nostromo behind one
# address, as behind a load balancer, and one node with one agent.
#  1. the agent, calling that one address, ends up connected to both instances
#  2. the node is reached through either instance
#  3. if one instance is killed, the node is still reached through the other
# Instance A listens on 127.0.0.1, B on 127.0.0.2, a small test balancer on
# this machine's non-loopback address. About 2 minutes.
source "$(dirname "$0")/lib.sh"
AGENT="$TOOLS/konnectivity/proxy-agent"
[ -x "$AGENT" ] && [ -x "$TOOLS/konnectivity/proxy-server" ] || { echo "Konnectivity binaries not found in $TOOLS/konnectivity"; exit 1; }
IP="$(node_ip)"
[ -n "$IP" ] || { echo "no non-loopback IPv4 address on this machine"; exit 1; }
( cd "$HERE" && "$TOOLS/go/bin/go" build -buildvcs=false -o "$D/lb" ./bench/lb ) || exit 1
A=127.0.0.1:7070; B=127.0.0.1:7071

"$N" datastore -dir "$D/ds" >"$D/ds.log" 2>&1 & DS=$!
EXTRA_PIDS="$DS"
for _ in $(seq 1 100); do grep -q "datastore ready" "$D/ds.log" && break; kill -0 $DS 2>/dev/null || { cat "$D/ds.log"; exit 1; }; nap 0.2; done

instance() { # instance LETTER LISTEN_HOST ADMIN
  "$N" serve -dir "$D/$1" -etcd https://127.0.0.1:12379 -pki "$D/ds/pki" -listen "$2:6443" -admin "$3" \
    -address "$IP" -node-address "$IP" -idle 60s -quiet 15s -warm 0 >>"$D/$1.log" 2>&1 &
  LAST=$!
  for _ in $(seq 1 100); do
    kill -0 $LAST 2>/dev/null || { echo "instance $1 did not start"; tail -5 "$D/$1.log"; exit 1; }
    grep -q "nostromo ready" "$D/$1.log" && return; nap 0.2
  done
}
instance a 127.0.0.1 $A; PA=$LAST
instance b 127.0.0.2 $B; PB=$LAST
EXTRA_PIDS="$DS $PA $PB"

"$N" create alpha -admin $A -dir "$D/a" >/dev/null; check $? "cluster created through instance A"
"$N" kubeconfig alpha -admin $A -dir "$D/a" >"$D/alpha.kc"
PORT="$(ADMIN=$A NOSTROMO_DIR="$D/a" cluster alpha port)"
AGENT_PORT="$(ADMIN=$A NOSTROMO_DIR="$D/a" cluster alpha agent_port)"
# through one instance, by its own address
via() { "$K" --kubeconfig "$D/alpha.kc" --server "https://$1:6443" --tls-server-name alpha.nostromo.local --request-timeout=15s "${@:2}"; }

# the shared address must exist before nodes, or the cluster's own setup, call it
"$D/lb" -listen "$IP" -ports "6443,$PORT,$AGENT_PORT" -backends 127.0.0.1,127.0.0.2 >"$D/lb.log" 2>&1 & LB=$!
EXTRA_PIDS="$DS $PA $PB $LB"
for _ in $(seq 1 50); do grep -q "lb ready" "$D/lb.log" && break; nap 0.2; done
JOIN="$("$N" join alpha -admin $A -dir "$D/a" 2>"$D/join.err")"; check $? "cluster prepared for nodes"
TOKEN="$(echo "$JOIN" | sed -n 's/.*--token \([a-z0-9.]*\).*/\1/p')"
TOLD="$(via 127.0.0.1 -n kube-system get daemonset konnectivity-agent -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null | sed -n 's/.*--proxy-server-port=\([0-9]*\).*/\1/p')"
[ -n "$AGENT_PORT" ] && [ "$TOLD" = "$AGENT_PORT" ]; check $? "agents are told to call $IP:$AGENT_PORT"
T="$D/a/tenants/alpha"

"$D/fakenode" -token "$TOKEN" -ca "$T/ca.crt" -server "$IP:$PORT" -name worker1 -ip "$IP" -kubelet-port 20250 >"$D/worker1.log" 2>&1 & NODE=$!
EXTRA_PIDS="$DS $PA $PB $LB $NODE"
for _ in $(seq 1 60); do [ "$(via 127.0.0.1 get node worker1 -o jsonpath='{.status.addresses[0].address}' 2>/dev/null)" = "$IP" ] && break; nap 1; done
reach() { via "$1" get --raw /api/v1/nodes/worker1/proxy/healthz 2>&1; }
reached() { local out; for _ in $(seq 1 "$2"); do out="$(reach "$1")"; [ "$out" = "hello from worker1" ] && return 0; nap 1; done; echo "  last answer: $(echo "$out" | head -1 | cut -c1-90)"; return 1; }

echo "one agent, two instances"
via 127.0.0.1 -n kube-system create token konnectivity-agent --audience system:konnectivity-server >"$D/agent.token"
"$AGENT" --logtostderr=true --ca-cert="$T/ca.crt" --proxy-server-host="$IP" --proxy-server-port="$AGENT_PORT" \
  --agent-identifiers="host=$IP" --sync-forever=true --service-account-token-path="$D/agent.token" \
  --admin-server-port=18133 --health-server-port=18134 >"$D/agent.log" 2>&1 & AG=$!
EXTRA_PIDS="$DS $PA $PB $LB $NODE $AG"
reached 127.0.0.1 60; check $? "the node is reached through instance A"
reached 127.0.0.2 60; check $? "the node is reached through instance B"
OK=0; for _ in $(seq 1 10); do [ "$(reach 127.0.0.1)" = "hello from worker1" ] && [ "$(reach 127.0.0.2)" = "hello from worker1" ] || OK=1; done
check $OK "ten times in a row, through both"

echo "instance A killed"
{ kill -9 $PA; wait $PA; } 2>/dev/null
EXTRA_PIDS="$DS $PB $LB $NODE $AG"
reached 127.0.0.2 20; check $? "the node is still reached through B"
nap 45   # A's claims expire; B learns it is alone and tells the agents
reached 127.0.0.2 30; check $? "and still is once B has noticed it is alone"
"$K" --kubeconfig "$D/alpha.kc" --request-timeout=15s get --raw /api/v1/nodes/worker1/proxy/healthz >/dev/null 2>&1
check $? "users going through the shared address are served too"
{ kill -9 $AG; wait $AG; } 2>/dev/null; EXTRA_PIDS="$DS $PB $LB $NODE"

verdict
