#!/bin/sh
# Start a throwaway Postgres next to the control room. Each run wipes the schema anyway,
# so durability doesn't matter here and fsync is off to keep runs fast.
set -e

PGBIN=$(ls -d /usr/lib/postgresql/*/bin | head -1)

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    "$PGBIN/initdb" -D "$PGDATA" -U app --auth=trust > /dev/null
fi
"$PGBIN/pg_ctl" -D "$PGDATA" -w -l /tmp/postgres.log start \
    -o "-c listen_addresses=localhost -c unix_socket_directories=/tmp -c max_connections=200 -c fsync=off -c synchronous_commit=off"
"$PGBIN/createdb" -h localhost -U app sync 2> /dev/null || true

exec python -m sim.server
