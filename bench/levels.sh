#!/usr/bin/env bash
# The three levels on a cluster IN USE (with nodes connected):
#  1. with nodes only sending "I am alive", the engines must stop
#  2. creating an application restarts the engines and pods get assigned to nodes
#  3. once the work is done the engines stop again, and heartbeats do not wake them
#  4. if a node stops answering, nostromo notices and restarts the engines,
#     which mark the node as not ready
# Takes about 4 minutes. Uses fake nodes (bench/fakenode). Memory: under 1 GB.
source "$(dirname "$0")/lib.sh"

show() { printf "  level=%-8s nodes=%s apiserver=%s MB engines=%s MB engine starts=%s (%s)\n" "$(cluster alpha level)" "$(cluster alpha nodes)" "$(cluster alpha api_mb)" "$(cluster alpha engines_mb)" "$(cluster alpha engine_starts)" "$(cluster alpha last_reason)"; }
wait_level() { for _ in $(seq 1 "$2"); do [ "$(cluster alpha level)" = "$1" ] && return 0; nap 1; done; return 1; }

up 1 -idle 30s -quiet 15s -node-grace 45s -v
"$N" create alpha >/dev/null
"$N" kubeconfig alpha >"$D/alpha.kc"
T="$D/data/tenants/alpha"
"$D/fakenode" -dir "$T" -sni alpha.nostromo.local -name node1 -status 20s >"$D/node1.log" 2>&1 & N1=$!
"$D/fakenode" -dir "$T" -sni alpha.nostromo.local -name node2 -status 20s >"$D/node2.log" 2>&1 & N2=$!
EXTRA_PIDS="$N1 $N2"

echo "1) cluster with 2 nodes connected, nobody doing anything"
wait_level standby 60; check $? "the engines stopped even though nodes keep sending heartbeats"
show

echo "2) create an application with 3 replicas"
k alpha create deployment web --image=nginx --replicas=3 >/dev/null
OK=1; ASSIGNED=0
for _ in $(seq 1 30); do
  ASSIGNED=$(k alpha get pods -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | grep -c node)
  [ "$ASSIGNED" = 3 ] && { OK=0; break; }; nap 1
done
check $OK "the engines restarted: 3 pods created and assigned to nodes ($ASSIGNED of 3)"
show

echo "3) work done"
wait_level standby 60; check $? "the engines stopped again"
S2=$(cluster alpha engine_starts)
nap 40
S3=$(cluster alpha engine_starts)
[ "$S2" = "$S3" ] && [ "$(cluster alpha level)" = standby ]; check $? "40 seconds of heartbeats alone did not wake them (starts: $S2 -> $S3)"
show

echo "4) kill node2 abruptly"
kill $N2; EXTRA_PIDS="$N1"
OK=1; ST=
for _ in $(seq 1 150); do
  ST=$(k alpha get node node2 -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
  [ "$ST" = Unknown ] && { OK=0; break; }; nap 1
done
check $OK "nostromo noticed the silence and the engines marked node2 as not ready (status: ${ST:-?})"
show
k alpha get nodes 2>&1 | sed 's/^/  /'

echo
grep -E "engines (started|stopped)" "$D/nostromo.log" | sed 's/^/  /'
verdict
