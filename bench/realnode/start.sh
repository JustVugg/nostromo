#!/usr/bin/env bash
# Step 1 of the real-node test (no root needed).
# Starts nostromo on this machine, creates the cluster "demo" and prepares it
# for a real worker node. Leaves nostromo running in the background.
# State lives in ~/nostromo-realnode. Stop everything with stop.sh.
set -eu
HERE="$(cd "$(dirname "$0")/../.." && pwd)"
TOOLS="${NOSTROMO_TOOLS:-$HOME/nostromo-tools}"
STATE="$HOME/nostromo-realnode"
N="$STATE/nostromo"
export NOSTROMO_DIR="$STATE/data"

IP="$(ip -4 -o addr show lo | awk '{print $4}' | cut -d/ -f1 | grep -v '^127\.' | head -1)"
[ -n "$IP" ] || IP="$(ip -4 -o addr show scope global | awk '{print $4}' | cut -d/ -f1 | head -1)"
[ -n "$IP" ] || { echo "no usable IPv4 address found"; exit 1; }

mkdir -p "$STATE"
if [ -f "$STATE/pid" ] && kill -0 "$(cat "$STATE/pid")" 2>/dev/null; then
  echo "nostromo is already running (pid $(cat "$STATE/pid")); run stop.sh first"; exit 1
fi
( cd "$HERE" && GOTOOLCHAIN=local GOPROXY=off "$TOOLS/go/bin/go" build -buildvcs=false -o "$N" . )

# A cluster with a node never sleeps, so the default timings are fine.
setsid nohup "$N" serve -dir "$STATE/data" -listen "$IP:6443" -address "$IP" -node-address "$IP" -v \
  >"$STATE/nostromo.log" 2>&1 </dev/null &
echo $! >"$STATE/pid"
for _ in $(seq 1 100); do
  kill -0 "$(cat "$STATE/pid")" 2>/dev/null || { echo "nostromo did not start:"; tail -5 "$STATE/nostromo.log"; exit 1; }
  grep -q "nostromo ready" "$STATE/nostromo.log" && break
  read -r -t 0.2 <> <(:) || true
done

"$N" list | grep -q '^demo ' || "$N" create demo >/dev/null
"$N" kubeconfig demo >"$STATE/demo.kubeconfig"
# A join command is good for 24 hours; preparing the cluster again would also
# put its add-ons back to their defaults.
if [ ! -s "$STATE/join.cmd" ] || [ -n "$(find "$STATE/join.cmd" -mmin +1200)" ]; then
  "$N" join demo >"$STATE/join.cmd"
fi

echo "nostromo is running on $IP (pid $(cat "$STATE/pid")), cluster 'demo' is ready for a node."
echo "join command saved in $STATE/join.cmd:"
echo "  $(cut -c1-60 "$STATE/join.cmd")..."
echo
echo "Next, in your own terminal:"
echo "  sudo bash $HERE/bench/realnode/install.sh"
