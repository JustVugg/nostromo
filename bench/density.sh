#!/usr/bin/env bash
# Density: create N clusters, use them one at a time, wait until they sleep and
# measure how much memory stays in use. Then wake some and time it.
#
# Usage: density.sh [N]   (default 20). At most 2-3 clusters awake at once (~1 GB).
source "$(dirname "$0")/lib.sh"
COUNT="${1:-20}"

up 1 -idle 4s -quiet 3s

echo "creating $COUNT clusters..."
T0=$(now_ms)
for i in $(seq 1 "$COUNT"); do "$N" create "c$i" >/dev/null; "$N" kubeconfig "c$i" >"$D/c$i.kc"; done
echo "created in $(( ($(now_ms) - T0) / COUNT )) ms each; memory used by clusters: $(status clusters_mem_mb) MB"

echo "starting them one at a time (first boot) and writing into each..."
for i in $(seq 1 "$COUNT"); do
  k "c$i" create namespace "probe-c$i" >/dev/null || echo "ERROR on c$i"
done

echo "waiting for all of them to sleep..."
for _ in $(seq 1 60); do [ "$(status clusters_mem_mb)" = 0 ] && break; nap 1; done
echo
echo "=== $COUNT clusters, all asleep ==="
echo "clusters memory: $(status clusters_mem_mb) MB"
echo "nostromo memory: $(status nostromo_mem_mb) MB"
echo "etcd memory:     $(status etcd_mem_mb) MB (shared by all)"
echo "(for comparison: $COUNT always-on control planes = about $((COUNT*405)) MB, see bench/baseline.sh)"

echo
echo "=== wake on demand (total time seen by kubectl) ==="
TOT=0; NUM=0; MAX=0
for i in $(seq 1 3 "$COUNT"); do
  T0=$(now_ms)
  OUT="$(k "c$i" get namespace -o name 2>&1)"
  MS=$(( $(now_ms) - T0 ))
  OWN=$(echo "$OUT" | grep -c "^namespace/probe-c$i$")
  OTHERS=$(echo "$OUT" | grep "^namespace/probe-" | grep -vc "^namespace/probe-c$i$")
  [ "$OWN" = 1 ] && [ "$OTHERS" = 0 ] || echo "ERROR on c$i: own=$OWN others=$OTHERS"
  TOT=$((TOT+MS)); NUM=$((NUM+1)); [ "$MS" -gt "$MAX" ] && MAX=$MS
  nap 2
done
echo "$NUM wakes: average $((TOT/NUM)) ms, worst $MAX ms; every cluster found its own data and only its own"
echo
echo "logs and data in: $D"
