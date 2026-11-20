#!/usr/bin/env bash
# The tunnel (Konnectivity): the control plane reaching a node it may not be
# able to dial directly, as needed by kubectl logs / exec and by webhooks.
#  1. with no agent connected, the control plane cannot reach the node
#  2. once the node's agent connects (authenticated by the cluster), it can
#  3. when the agent goes away, it cannot any more: traffic really goes
#     through the tunnel and nowhere else
# The agent is the upstream binary run as a local process; on a real node it
# runs as a pod, from the DaemonSet that "nostromo join" installs.
# Needs ~/nostromo-tools/konnectivity/{proxy-server,proxy-agent}. About 1 minute.
source "$(dirname "$0")/lib.sh"
AGENT="$TOOLS/konnectivity/proxy-agent"
[ -x "$AGENT" ] && [ -x "$TOOLS/konnectivity/proxy-server" ] || { echo "Konnectivity binaries not found in $TOOLS/konnectivity"; exit 1; }
IP="$(node_ip)"
[ -n "$IP" ] || { echo "no non-loopback IPv4 address on this machine"; exit 1; }

up 1 -listen "$IP:6443" -address "$IP" -node-address "$IP" -idle 60s -quiet 15s
"$N" create alpha >/dev/null
"$N" kubeconfig alpha >"$D/alpha.kc"
T="$D/data/tenants/alpha"
JOIN="$("$N" join alpha 2>"$D/join.err")"; check $? "cluster prepared for nodes"
[ -s "$D/join.err" ] && sed 's/^/  /' "$D/join.err"
PORT="$(cluster alpha port)"
TOKEN="$(echo "$JOIN" | sed -n 's/.*--token \([a-z0-9.]*\).*/\1/p')"
AGENT_PORT="$(k alpha -n kube-system get daemonset konnectivity-agent -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null | sed -n 's/.*--proxy-server-port=\([0-9]*\).*/\1/p')"
[ -n "$AGENT_PORT" ] && [ "$AGENT_PORT" != "$PORT" ]; check $? "the agent to run on every node is installed, pointed at $IP:$AGENT_PORT"
[ "$(cluster alpha tunnel_mb)" -gt 0 ]; check $? "a tunnel server runs next to the apiserver ($(cluster alpha tunnel_mb) MB)"

"$D/fakenode" -token "$TOKEN" -ca "$T/ca.crt" -server "$IP:$PORT" -name worker1 -ip "$IP" -kubelet-port 20250 >"$D/worker1.log" 2>&1 & NODE=$!
EXTRA_PIDS="$NODE"
for _ in $(seq 1 60); do [ "$(k alpha get node worker1 -o jsonpath='{.status.addresses[0].address}' 2>/dev/null)" = "$IP" ] && break; nap 1; done
reach() { "$K" --kubeconfig "$D/alpha.kc" --request-timeout=15s get --raw /api/v1/nodes/worker1/proxy/healthz 2>&1; }

echo "no agent connected"
OUT="$(reach)"; [ "$OUT" != "hello from worker1" ]; check $? "the control plane cannot reach the node ($(echo "$OUT" | head -1 | cut -c1-70))"

echo "agent connected"
k alpha -n kube-system create token konnectivity-agent --audience system:konnectivity-server >"$D/agent.token"; check $? "the cluster issues the agent its token"
"$AGENT" --logtostderr=true --ca-cert="$T/ca.crt" --proxy-server-host="$IP" --proxy-server-port="$AGENT_PORT" \
  --agent-identifiers="host=$IP" --service-account-token-path="$D/agent.token" --admin-server-port=18133 --health-server-port=18134 >"$D/agent.log" 2>&1 & AG=$!
EXTRA_PIDS="$NODE $AG"
OK=1; for _ in $(seq 1 30); do OUT="$(reach)"; [ "$OUT" = "hello from worker1" ] && { OK=0; break; }; nap 1; done
check $OK "the control plane reaches the node through the tunnel (answer: $(echo "$OUT" | head -1 | cut -c1-50))"
echo "bad" >"$D/bad.token"
"$AGENT" --logtostderr=true --ca-cert="$T/ca.crt" --proxy-server-host="$IP" --proxy-server-port="$AGENT_PORT" \
  --service-account-token-path="$D/bad.token" --admin-server-port=18135 --health-server-port=18136 >"$D/bad-agent.log" 2>&1 & BAD=$!
nap 4; { kill -9 $BAD; wait $BAD; } 2>/dev/null
grep -qi "unauthenticated\|failed to authenticate\|authentication" "$D/bad-agent.log" "$T/konnectivity.log"; check $? "an agent with a made-up token is turned away"

echo "agent gone"
{ kill -9 $AG; wait $AG; } 2>/dev/null; EXTRA_PIDS="$NODE"   # a plain signal only makes the agent drain slowly
OK=1; for _ in $(seq 1 30); do OUT="$(reach)"; [ "$OUT" != "hello from worker1" ] && { OK=0; break; }; nap 1; done
check $OK "without the agent the node is out of reach again: the traffic had no other way"

verdict
