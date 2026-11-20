#!/usr/bin/env bash
# End-to-end check: isolation between clusters, sleep and wake, restart, deletion.
# Takes about 1 minute. Memory: under 1 GB.
source "$(dirname "$0")/lib.sh"
ETCDCTL="$TOOLS/etcd/etcdctl"
etcdctl() { "$ETCDCTL" --endpoints=https://127.0.0.1:12379 --cacert="$D/data/pki/etcd-ca.crt" --command-timeout=5s "$@"; }
all_asleep() { for _ in $(seq 1 40); do [ "$(status clusters_mem_mb)" = 0 ] && return 0; nap 1; done; return 1; }

up 1 -idle 4s -quiet 3s
"$N" create alpha >/dev/null && "$N" create beta >/dev/null
"$N" kubeconfig alpha >"$D/alpha.kc"; "$N" kubeconfig beta >"$D/beta.kc"

echo "isolation"
k alpha create namespace secret-alpha >/dev/null; check $? "alpha: writes data (started on demand)"
k beta create namespace secret-beta >/dev/null; check $? "beta: writes data"
not k beta get namespace secret-alpha; check $? "beta cannot see alpha's data"
# alpha's credentials presented to beta
sed 's/tls-server-name: alpha\./tls-server-name: beta./' "$D/alpha.kc" >"$D/mixed.kc"
not k mixed get namespaces; check $? "alpha's credentials do not open beta"
not "$N" create alpha; check $? "the same name cannot be created twice"
not "$N" create "../x"; check $? "dangerous names are rejected"

echo "shared datastore"
CA="--cert=$D/data/tenants/alpha/etcd.crt --key=$D/data/tenants/alpha/etcd.key"
[ "$(etcdctl $CA get --prefix /nostromo/alpha/namespaces/ --keys-only 2>/dev/null | grep -c secret-alpha)" = 1 ]; check $? "alpha's key reads alpha's slice"
not etcdctl $CA get --prefix /nostromo/beta/ --keys-only; check $? "alpha's key CANNOT read beta's slice"
not etcdctl $CA put /nostromo/beta/x y; check $? "alpha's key CANNOT write into beta's slice"
not etcdctl get --prefix /nostromo/ --keys-only; check $? "without a key nothing can be read"

echo "sleep and wake"
all_asleep; check $? "with no connections both clusters go to sleep (0 MB)"
[ "$(k alpha get namespace secret-alpha -o name 2>/dev/null)" = namespace/secret-alpha ]; check $? "alpha wakes on its own and finds its data"
[ "$(cluster beta level)" = asleep ]; check $? "beta stays asleep"

echo "nostromo restart"
kill $PID; wait $PID 2>/dev/null
up 2 -idle 4s -quiet 3s
[ "$(k beta get namespace secret-beta -o name 2>/dev/null)" = namespace/secret-beta ]; check $? "after the restart beta is still there, with its data and the same kubeconfig"

echo "deletion"
"$N" delete alpha >/dev/null; check $? "alpha deleted"
not k alpha get namespaces; check $? "alpha no longer answers"
R="--cert=$D/data/pki/etcd-root.crt --key=$D/data/pki/etcd-root.key --user=root:$(cat "$D/data/pki/etcd-root.pw")"
[ "$(etcdctl $R get --prefix /nostromo/alpha/ --keys-only 2>/dev/null | grep -c .)" = 0 ]; check $? "no data of alpha is left in the datastore"
[ "$(etcdctl $R get --prefix /nostromo/beta/ --keys-only 2>/dev/null | grep -c .)" -gt 100 ]; check $? "beta's data is intact"
[ ! -e "$D/data/tenants/alpha" ]; check $? "alpha's keys and certificates removed from disk"

echo "first boot in the background"
kill $PID; wait $PID 2>/dev/null
up 3 -idle 60s -quiet 3s -warm 1
"$N" create gamma >/dev/null; "$N" kubeconfig gamma >"$D/gamma.kc"
OK=1; for _ in $(seq 1 60); do [ "$(cluster gamma wakes)" = 1 ] && [ "$(cluster gamma level)" = asleep ] && { OK=0; break; }; nap 1; done
check $OK "a new cluster boots once on its own and goes back to sleep without waiting for the idle time"
k gamma get serviceaccount default >/dev/null 2>&1; check $? "its first user finds it already initialised"
[ "$(cluster gamma last_wake_ms)" -lt 8000 ]; check $? "and waits only a normal wake-up ($(cluster gamma last_wake_ms) ms)"

verdict
