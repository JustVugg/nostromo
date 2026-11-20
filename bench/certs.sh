#!/usr/bin/env bash
# Certificates over time. Certificates last a year and are renewed a month
# before they expire; here they last two minutes, so that six minutes show
# three lifetimes go by on a cluster that never restarts:
#  - the cluster keeps answering users, and nostromo's own commands keep working
#  - its engines keep doing their work
#  - the certificates on disk really are replaced
# The cluster has a (fake) node, so it never sleeps and nothing is restarted.
# Takes about 7 minutes.
source "$(dirname "$0")/lib.sh"
IP="$(node_ip)"
[ -n "$IP" ] || { echo "no non-loopback IPv4 address on this machine"; exit 1; }
export NOSTROMO_TEST_CERT_LIFETIME=2m

up 1 -listen "$IP:6443" -address "$IP" -node-address "$IP" -idle 60s -quiet 15s
"$N" create alpha >/dev/null
"$N" kubeconfig alpha >"$D/alpha.kc"
T="$D/data/tenants/alpha"
JOIN="$("$N" join alpha 2>"$D/join.err")"; check $? "cluster prepared for nodes"
PORT="$(cluster alpha port)"
TOKEN="$(echo "$JOIN" | sed -n 's/.*--token \([a-z0-9.]*\).*/\1/p')"
"$D/fakenode" -token "$TOKEN" -ca "$T/ca.crt" -server "$IP:$PORT" -name worker1 >"$D/worker1.log" 2>&1 & NODE=$!
EXTRA_PIDS="$NODE"
for _ in $(seq 1 60); do k alpha get node worker1 >/dev/null 2>&1 && break; nap 1; done
k alpha get node worker1 >/dev/null 2>&1; check $? "a node joined: the cluster will not sleep"

serial() { openssl x509 -in "$1" -noout -serial 2>/dev/null; }
declare -A FIRST
FILES="$T/apiserver.crt $T/admin.crt $T/cm.crt $T/sched.crt $T/etcd.crt $T/front-proxy-client.crt $D/data/pki/etcd-server.crt $D/data/pki/etcd-root.crt"
for f in $FILES; do FIRST[$f]="$(serial "$f")"; done
WAKES="$(cluster alpha wakes)"

echo "six minutes, a check every 20 seconds"
BAD_API=0; BAD_ADMIN=0; BAD_ENGINES=0; ROUNDS=18
for i in $(seq 1 $ROUNDS); do
  nap 20
  "$N" kubeconfig alpha >"$D/alpha.kc" 2>/dev/null || BAD_ADMIN=$((BAD_ADMIN+1))   # a user's file lasts as long as a certificate
  k alpha create configmap "round-$i" --from-literal=n="$i" >/dev/null 2>&1 || BAD_API=$((BAD_API+1))
  # work only the engines can do: a deployment must get its pod
  k alpha create deployment "d$i" --image=none --replicas=1 >/dev/null 2>&1
  OK=1; for _ in $(seq 1 15); do [ "$(k alpha get pods -l app="d$i" --no-headers 2>/dev/null | wc -l)" = 1 ] && { OK=0; break; }; nap 1; done
  [ "$OK" = 0 ] || BAD_ENGINES=$((BAD_ENGINES+1))
done
check $BAD_API "users were served at every one of the $ROUNDS checks (failures: $BAD_API)"
check $BAD_ADMIN "nostromo's own commands worked every time (failures: $BAD_ADMIN)"
check $BAD_ENGINES "the engines did their work every time (failures: $BAD_ENGINES)"
SAME=0; for f in $FILES; do [ "$(serial "$f")" != "${FIRST[$f]}" ] || { SAME=$((SAME+1)); echo "  not renewed: $f"; }; done
check $SAME "every certificate on disk was replaced"
[ "$(cluster alpha wakes)" = "$WAKES" ]; check $? "the cluster was never restarted (apiserver starts: $(cluster alpha wakes))"
"$K" --kubeconfig "$D/alpha.kc" --request-timeout=15s get --raw /readyz >/dev/null 2>&1; check $? "and it is still answering"

verdict
