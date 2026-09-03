#!/bin/bash
# SessionStart hook for Claude Code on the web only: installs gems, makes sure PostgreSQL is
# running, and relaxes local TCP auth to trust so the test suite (which connects via both a
# Unix socket and postgres://localhost/...) can reach it without a password. A no-op anywhere
# else (e.g. a local `claude` session), since CLAUDE_CODE_REMOTE is only set on the web.
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"

bundle install

bin/ensure-postgres

# Allow trust auth for TCP connections to localhost/127.0.0.1/::1, so a `postgres://localhost/...`
# connection (used by one of the tests) works the same as the local socket connection does.
for hba in /etc/postgresql/*/main/pg_hba.conf; do
  [ -f "$hba" ] || continue

  sed -i \
    -e 's/^\(host\s\+all\s\+all\s\+127\.0\.0\.1\/32\s\+\)\S\+/\1trust/' \
    -e 's/^\(host\s\+all\s\+all\s\+::1\/128\s\+\)\S\+/\1trust/' \
    "$hba"
done

if command -v pg_ctlcluster >/dev/null 2>&1 && [ -d /etc/postgresql ]; then
  for version_dir in /etc/postgresql/*/; do
    [ -d "$version_dir" ] || continue
    version="$(basename "$version_dir")"
    pg_ctlcluster "$version" main reload >/dev/null 2>&1 || true
  done
elif command -v service >/dev/null 2>&1; then
  service postgresql reload >/dev/null 2>&1 || true
fi
