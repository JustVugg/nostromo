# Shared helpers for the bench scripts. Source it; it builds the binaries into
# a fresh temp directory $D and defines helpers to run nostromo locally.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS="${NOSTROMO_TOOLS:-$HOME/nostromo-tools}"
K="$TOOLS/k8s/kubectl"
D="$(mktemp -d "${TMPDIR:-/tmp}/nostromo-bench.XXXXXX")"
N="$D/nostromo"
ADMIN=127.0.0.1:7070
export NOSTROMO_DIR="$D/data"   # where the commands find the API's token
export GOTOOLCHAIN=local GOPROXY=off

( cd "$HERE" && "$TOOLS/go/bin/go" build -buildvcs=false -o "$N" . \
  && "$TOOLS/go/bin/go" build -buildvcs=false -o "$D/fakenode" ./bench/fakenode ) || exit 1

PID=; EXTRA_PIDS=
trap 'kill $EXTRA_PIDS $PID 2>/dev/null; wait 2>/dev/null' EXIT

nap() { read -r -t "$1" <> <(:) || true; }
now_ms() { date +%s%3N; }

# up N_READY [serve options...]: start nostromo and wait for its N-th "ready" line.
up() {
  local want=$1; shift
  "$N" serve -dir "$D/data" -admin "$ADMIN" -warm 0 "$@" >>"$D/nostromo.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do
    kill -0 $PID 2>/dev/null || { echo "nostromo did not start"; tail -5 "$D/nostromo.log"; exit 1; }
    [ "$(grep -c "nostromo ready" "$D/nostromo.log")" = "$want" ] && return
    nap 0.2
  done
  echo "nostromo did not become ready"; exit 1
}

# status FIELD: a top-level field of the status document.
status() { "$N" status -admin "$ADMIN" | python3 -I -c "import sys,json; print(json.load(sys.stdin)['$1'])"; }
# cluster NAME FIELD: a field of one cluster.
cluster() { "$N" status -admin "$ADMIN" | python3 -I -c "import sys,json; print(*[t['$2'] for t in json.load(sys.stdin)['list'] if t['name']=='$1'])"; }
# k NAME args...: kubectl against a cluster (kubeconfig in $D/NAME.kc).
k() { local c=$1; shift; "$K" --kubeconfig "$D/$c.kc" --request-timeout=60s "$@"; }

# node_ip: an IPv4 address of this machine that is not 127.x (nodes and pods
# cannot be given a loopback address). Prefers one that stays on this machine.
node_ip() {
  local a
  a="$(ip -4 -o addr show lo 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | grep -v '^127\.' | head -1)"
  [ -n "$a" ] || a="$(ip -4 -o addr show scope global 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | head -1)"
  echo "$a"
}

FAILED=0
check() { if [ "$1" = 0 ]; then echo "  OK: $2"; else echo "  FAILED: $2"; FAILED=$((FAILED+1)); fi; }
not() { if "$@" >/dev/null 2>&1; then return 1; else return 0; fi; }
verdict() { echo; if [ "$FAILED" = 0 ]; then echo "ALL OK"; else echo "$FAILED checks failed - logs in $D"; fi; exit "$FAILED"; }
