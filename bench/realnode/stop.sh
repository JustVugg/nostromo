#!/usr/bin/env bash
# Stops the nostromo started by start.sh (no root needed). With "--purge" it
# also deletes ~/nostromo-realnode, the cluster "demo" included.
# Run "sudo bash uninstall.sh" first if a node was installed.
set -u
STATE="$HOME/nostromo-realnode"
if [ -f "$STATE/pid" ] && kill -0 "$(cat "$STATE/pid")" 2>/dev/null; then
  kill "$(cat "$STATE/pid")"
  for _ in $(seq 1 50); do kill -0 "$(cat "$STATE/pid")" 2>/dev/null || break; read -r -t 0.2 <> <(:) || true; done
  echo "nostromo stopped"
else
  echo "nostromo was not running"
fi
rm -f "$STATE/pid"
[ "${1:-}" = "--purge" ] && rm -rf "$STATE" && echo "$STATE deleted"
exit 0
