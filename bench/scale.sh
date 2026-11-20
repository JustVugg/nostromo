#!/usr/bin/env bash
# Scale: how nostromo and the shared datastore grow with the number of clusters.
#  - BOOTED clusters are started once, written to, and left to sleep
#    (each leaves its full initial state in the datastore)
#  - EXTRA clusters are only created, never started
# Usage: scale.sh [BOOTED] [EXTRA]   (default 100 and 1000)
# Two clusters are started at a time: peak memory stays under 2 GB.
source "$(dirname "$0")/lib.sh"
BOOTED="${1:-100}"; EXTRA="${2:-1000}"
dbsize() { du -sm "$D/data/etcd" | cut -f1; }

up 1 -idle 4s -quiet 3s -ports 30000-49999
echo "empty: nostromo $(status nostromo_mem_mb) MB, datastore $(status etcd_mem_mb) MB"

T0=$(now_ms)
for i in $(seq 1 "$BOOTED"); do "$N" create "c$i" >/dev/null; "$N" kubeconfig "c$i" >"$D/c$i.kc"; done
echo "$BOOTED clusters created in $(( ($(now_ms) - T0) / BOOTED )) ms each"

echo "starting them two at a time..."
T0=$(now_ms)
seq 1 "$BOOTED" | xargs -P 2 -I{} "$K" --kubeconfig "$D/c{}.kc" --request-timeout=120s create namespace "probe-c{}" >/dev/null 2>"$D/boot.err"
echo "all started in $(( ($(now_ms) - T0) / 1000 )) s; errors: $(grep -c . "$D/boot.err")"
for _ in $(seq 1 120); do [ "$(status clusters_mem_mb)" = 0 ] && break; nap 1; done

echo
echo "=== $BOOTED clusters started once, all asleep ==="
echo "clusters memory:  $(status clusters_mem_mb) MB"
echo "nostromo memory:  $(status nostromo_mem_mb) MB"
echo "datastore memory: $(status etcd_mem_mb) MB  (on disk: $(dbsize) MB)"

T0=$(now_ms)
for i in $(seq 1 "$EXTRA"); do "$N" create "x$i" >/dev/null || echo "ERROR creating x$i"; done
echo
echo "=== plus $EXTRA clusters created and never started ($(( ($(now_ms) - T0) / EXTRA )) ms each) ==="
echo "clusters known:   $(status clusters)"
echo "nostromo memory:  $(status nostromo_mem_mb) MB"
echo "datastore memory: $(status etcd_mem_mb) MB  (on disk: $(dbsize) MB)"

echo
echo "=== wake on demand with $((BOOTED + EXTRA)) clusters defined ==="
TOT=0; NUM=0; MAX=0; MISSING=0; LEAKED=0
for i in $(seq 1 $((BOOTED / 10)) "$BOOTED"); do
  T0=$(now_ms)
  OUT="$(k "c$i" get namespace -o name 2>&1)"
  MS=$(( $(now_ms) - T0 ))
  OWN=$(echo "$OUT" | grep -c "^namespace/probe-c$i$")
  OTHERS=$(echo "$OUT" | grep "^namespace/probe-" | grep -vc "^namespace/probe-c$i$")
  [ "$OWN" = 1 ] || MISSING=$((MISSING+1))   # its first write failed earlier (see boot errors)
  [ "$OTHERS" = 0 ] || LEAKED=$((LEAKED+1))  # it sees another cluster's data: must never happen
  TOT=$((TOT+MS)); NUM=$((NUM+1)); [ "$MS" -gt "$MAX" ] && MAX=$MS
  nap 2
done
echo "$NUM wakes: average $((TOT/NUM)) ms, worst $MAX ms; own data missing: $MISSING, other clusters' data visible: $LEAKED"
echo
echo "logs and data in: $D"
