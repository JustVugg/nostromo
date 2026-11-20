#!/usr/bin/env bash
# Several instances: two copies of nostromo share one datastore.
#  1. a cluster created through one instance appears on the other
#  2. both serve the same cluster with the same data and the same credentials
#  3. only one of them runs the cluster's engines at any time
#  4. if that instance is killed, the other one takes the work over
#  5. a cluster deleted through one instance disappears from the other
# Both instances run on this machine, so only the first can open the clusters'
# dedicated ports; on separate machines each would. Takes about 2 minutes.
source "$(dirname "$0")/lib.sh"
A=127.0.0.1:7070; B=127.0.0.1:7071

"$N" datastore -dir "$D/ds" >"$D/ds.log" 2>&1 & DS=$!
EXTRA_PIDS="$DS"
for _ in $(seq 1 100); do grep -q "datastore ready" "$D/ds.log" && break; kill -0 $DS 2>/dev/null || { cat "$D/ds.log"; exit 1; }; nap 0.2; done

instance() { # instance LETTER SHARED_PORT ADMIN
  "$N" serve -dir "$D/$1" -etcd https://127.0.0.1:12379 -pki "$D/ds/pki" -listen "127.0.0.1:$2" -admin "$3" \
    -idle 60s -quiet 10s -warm 0 -v >>"$D/$1.log" 2>&1 &
  LAST=$!
  for _ in $(seq 1 100); do
    kill -0 $LAST 2>/dev/null || { echo "instance $1 did not start"; tail -5 "$D/$1.log"; exit 1; }
    grep -q "nostromo ready" "$D/$1.log" && [ "$(curl -m 2 -s "http://$3/status" | grep -c instance)" = 1 ] && return; nap 0.2
  done
}
instance a 6443 $A; PA=$LAST
instance b 6444 $B; PB=$LAST
EXTRA_PIDS="$DS $PA $PB"
on() { local adm=$1; shift; ADMIN=$adm "$@"; }
full_count() { echo $(( $( [ "$(on $A cluster alpha level)" = full ] && echo 1 || echo 0 ) + $( [ "$(on $B cluster alpha level)" = full ] && echo 1 || echo 0 ) )); }

echo "shared cluster list"
"$N" create alpha -admin $A >/dev/null; check $? "cluster created through instance A"
OK=1; for _ in $(seq 1 20); do [ "$(on $B cluster alpha level)" = asleep ] && { OK=0; break; }; nap 0.5; done
check $OK "instance B knows the cluster too"
not "$N" create alpha -admin $B; check $? "B refuses to create the same name again"

echo "same cluster from both"
"$N" kubeconfig alpha -admin $A >"$D/alpha.kc"
"$N" kubeconfig alpha -admin $B >"$D/alphab.kc"
k alpha create namespace made-via-a >/dev/null; check $? "data written through A"
[ "$(k alphab get namespace made-via-a -o name 2>/dev/null)" = namespace/made-via-a ]; check $? "the same data is read through B"
sed 's/127.0.0.1:6443/127.0.0.1:6444/' "$D/alpha.kc" >"$D/cross.kc"
k cross get namespaces >/dev/null 2>&1; check $? "the credentials handed out by A are accepted by B"

echo "one set of engines"
MAX=0; SEEN=0
for _ in $(seq 1 16); do
  k alpha get ns >/dev/null 2>&1; k alphab get ns >/dev/null 2>&1   # keep both awake
  C=$(full_count); [ "$C" -gt "$MAX" ] && MAX=$C; [ "$C" = 1 ] && SEEN=1
  nap 0.5
done
[ "$MAX" -le 1 ] && [ "$SEEN" = 1 ]; check $? "with the cluster awake on both, the engines ran on exactly one instance (max at once: $MAX)"

echo "takeover"
kill -9 $PA; EXTRA_PIDS="$DS $PB"
k alphab create deployment web --image=nginx --replicas=3 >/dev/null; check $? "instance A killed; B accepts new work"
OK=1; PODS=0
for _ in $(seq 1 90); do
  PODS=$(k alphab get pods -l app=web -o name 2>/dev/null | grep -c pod/)
  [ "$PODS" = 3 ] && { OK=0; break; }; nap 1
done
check $OK "B's engines took over and created the 3 pods ($PODS of 3)"
echo "  B: level=$(on $B cluster alpha level) reason=$(on $B cluster alpha last_reason)"

echo "deletion seen everywhere"
instance a 6443 $A; PA=$LAST; EXTRA_PIDS="$DS $PA $PB"
[ "$(on $A cluster alpha level)" != "" ]; check $? "A restarted and found the cluster in the datastore"
"$N" delete alpha -admin $B >/dev/null; check $? "cluster deleted through B"
OK=1; for _ in $(seq 1 20); do [ "$(on $A cluster alpha level)" = "" ] && { OK=0; break; }; nap 0.5; done
check $OK "A dropped it too"

verdict
