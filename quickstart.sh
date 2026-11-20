#!/usr/bin/env bash
# Try nostromo in one command, on any Linux machine, without root.
#
#   ./quickstart.sh         fetch what is missing, build, start nostromo,
#                           create a cluster and show it answering
#   ./quickstart.sh stop    stop it (add --purge to delete its data too)
#
# What it fetches, once, into ~/nostromo-tools (or $NOSTROMO_TOOLS), each
# file checked against the checksum its publisher provides:
#   - Kubernetes $K8S (apiserver, controller-manager, scheduler, kubeadm, kubectl)
#   - etcd $ETCD
#   - the Go toolchain, only if there is no recent "go" on this machine
#   - the Konnectivity tunnel server, built from its source
# Nothing is installed system-wide; delete that directory and ./data to undo.
set -euo pipefail

K8S=v1.37.2
ETCD=v3.7.4
GO=1.27.4
KONN=v0.37.1

HERE="$(cd "$(dirname "$0")" && pwd)"
TOOLS="${NOSTROMO_TOOLS:-$HOME/nostromo-tools}"
DATA="${NOSTROMO_DIR:-$HERE/data}"
LISTEN="${NOSTROMO_LISTEN:-127.0.0.1:6443}"
ADMIN="${NOSTROMO_ADMIN:-127.0.0.1:7070}"
N="$HERE/nostromo"
export NOSTROMO_DIR="$DATA"

say() { printf '\n== %s\n' "$*"; }
die() { echo "error: $*" >&2; exit 1; }
nap() { read -r -t "$1" <> <(:) || true; }

if [ "${1:-}" = stop ]; then
  if [ -f "$DATA/nostromo.pid" ] && kill -0 "$(cat "$DATA/nostromo.pid")" 2>/dev/null; then
    kill "$(cat "$DATA/nostromo.pid")"
    for _ in $(seq 1 50); do kill -0 "$(cat "$DATA/nostromo.pid")" 2>/dev/null || break; nap 0.2; done
    echo "nostromo stopped"
  else
    echo "nostromo was not running"
  fi
  rm -f "$DATA/nostromo.pid"
  if [ "${2:-}" = --purge ]; then rm -rf "$DATA"; echo "$DATA deleted"; fi
  exit 0
fi

[ "$(uname -s)" = Linux ] || die "nostromo runs on Linux"
case "$(uname -m)" in
  x86_64) ARCH=amd64 ;;
  aarch64 | arm64) ARCH=arm64 ;;
  *) die "unsupported processor: $(uname -m)" ;;
esac
for c in curl tar sha256sum; do command -v "$c" >/dev/null || die "'$c' is needed"; done

# fetch URL FILE: download, resuming and retrying
fetch() { curl -fL --retry 3 --retry-delay 2 -sS -o "$2" "$1" || die "cannot download $1"; }
# verify FILE SHA256
verify() {
  [ "$(sha256sum "$1" | cut -d' ' -f1)" = "$2" ] || { rm -f "$1"; die "checksum mismatch for $1"; }
}

mkdir -p "$TOOLS/k8s" "$TOOLS/etcd" "$TOOLS/konnectivity"

say "Kubernetes $K8S"
for b in kube-apiserver kube-controller-manager kube-scheduler kubeadm kubectl; do
  if [ -x "$TOOLS/k8s/$b" ]; then echo "  $b: already there"; continue; fi
  echo "  $b: downloading..."
  url="https://dl.k8s.io/$K8S/bin/linux/$ARCH/$b"
  fetch "$url" "$TOOLS/k8s/$b.part"
  verify "$TOOLS/k8s/$b.part" "$(curl -fsSL "$url.sha256" | cut -d' ' -f1)"
  chmod +x "$TOOLS/k8s/$b.part"; mv "$TOOLS/k8s/$b.part" "$TOOLS/k8s/$b"
done

say "etcd $ETCD"
if [ -x "$TOOLS/etcd/etcd" ]; then echo "  already there"; else
  echo "  downloading..."
  f="etcd-$ETCD-linux-$ARCH.tar.gz"; rel="https://github.com/etcd-io/etcd/releases/download/$ETCD"
  fetch "$rel/$f" "$TOOLS/etcd/$f"
  sum="$(curl -fsSL "$rel/SHA256SUMS" | awk -v f="$f" '$2 == f {print $1}')"
  [ -n "$sum" ] || die "no published checksum for $f"
  verify "$TOOLS/etcd/$f" "$sum"
  tar -xzf "$TOOLS/etcd/$f" -C "$TOOLS/etcd" --strip-components=1 --no-same-owner
  rm -f "$TOOLS/etcd/$f"
fi

say "Go"
GOBIN=""
if [ -x "$TOOLS/go/bin/go" ]; then GOBIN="$TOOLS/go/bin/go"
elif command -v go >/dev/null && [ "$(printf '%s\n' 1.27 "$(go env GOVERSION | sed 's/^go//')" | sort -V | head -1)" = 1.27 ]; then GOBIN="$(command -v go)"
fi
if [ -n "$GOBIN" ]; then echo "  using $GOBIN"; else
  echo "  downloading the toolchain $GO..."
  f="go$GO.linux-$ARCH.tar.gz"
  fetch "https://dl.google.com/go/$f" "$TOOLS/$f"
  verify "$TOOLS/$f" "$(curl -fsSL "https://dl.google.com/go/$f.sha256")"
  tar -xzf "$TOOLS/$f" -C "$TOOLS" --no-same-owner
  rm -f "$TOOLS/$f"
  GOBIN="$TOOLS/go/bin/go"
fi

say "tunnel server (Konnectivity $KONN)"
if [ -x "$TOOLS/konnectivity/proxy-server" ]; then echo "  already there"; else
  echo "  building from source (a minute or two)..."
  src="$TOOLS/konnectivity/src"
  rm -rf "$src"; mkdir -p "$src"
  if curl -fsSL "https://github.com/kubernetes-sigs/apiserver-network-proxy/archive/refs/tags/$KONN.tar.gz" \
       | tar -xz -C "$src" --strip-components=1 --no-same-owner \
     && ( cd "$src" && GOFLAGS=-mod=mod CGO_ENABLED=0 "$GOBIN" build -o "$TOOLS/konnectivity/proxy-server" ./cmd/server ) >"$TOOLS/konnectivity/build.log" 2>&1; then
    rm -rf "$src"
  else
    echo "  could not build it (see $TOOLS/konnectivity/build.log): nostromo works, but"
    echo "  logs/exec towards nodes the control plane cannot dial will not."
  fi
fi

say "building nostromo"
( cd "$HERE" && GOTOOLCHAIN=local "$GOBIN" build -buildvcs=false -o "$N" . )
echo "  $N"

say "starting nostromo"
mkdir -p "$DATA"
if [ -f "$DATA/nostromo.pid" ] && kill -0 "$(cat "$DATA/nostromo.pid")" 2>/dev/null; then
  echo "  already running (pid $(cat "$DATA/nostromo.pid"))"
else
  : >"$DATA/nostromo.log"
  setsid nohup "$N" serve -dir "$DATA" -tools "$TOOLS" -listen "$LISTEN" -admin "$ADMIN" \
    >>"$DATA/nostromo.log" 2>&1 </dev/null &
  echo $! >"$DATA/nostromo.pid"
  for _ in $(seq 1 150); do
    kill -0 "$(cat "$DATA/nostromo.pid")" 2>/dev/null || { tail -5 "$DATA/nostromo.log" >&2; die "nostromo did not start (is $LISTEN or $ADMIN already in use?)"; }
    grep -q "nostromo ready" "$DATA/nostromo.log" && break
    nap 0.2
  done
  grep -q "nostromo ready" "$DATA/nostromo.log" || die "nostromo did not become ready: see $DATA/nostromo.log"
  echo "  running (pid $(cat "$DATA/nostromo.pid")), log in $DATA/nostromo.log"
fi

say "a first cluster"
"$N" list -admin "$ADMIN" | grep -q '^demo ' || "$N" create demo -admin "$ADMIN" >/dev/null
"$N" kubeconfig demo -admin "$ADMIN" >"$HERE/demo.kubeconfig"
chmod 600 "$HERE/demo.kubeconfig"
KC="$TOOLS/k8s/kubectl --kubeconfig $HERE/demo.kubeconfig"
ok=1
for _ in $(seq 1 30); do $KC get namespaces >/dev/null 2>&1 && { ok=0; break; }; nap 2; done
[ "$ok" = 0 ] || die "the cluster did not answer: see $DATA/tenants/demo/apiserver.log"
$KC get namespaces
echo
"$N" list -admin "$ADMIN"

cat <<EOF

It works. The cluster "demo" is a real Kubernetes control plane: it started
when kubectl called it and will go back to sleep, using no memory, after five
minutes without connections.

  $KC get namespaces
  NOSTROMO_DIR=$DATA $N list
  NOSTROMO_DIR=$DATA $N create another
  $0 stop

Worker nodes, several instances and every option: see README.md.
EOF
