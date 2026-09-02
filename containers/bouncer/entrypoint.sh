#!/bin/bash
set -e

export PGPASSWORD="$MUNINN_DB_READONLY_PASSWORD"
ROHASH=$( \
psql -h "$MUNINN_DB_HOST" -p "$MUNINN_DB_PORT" -d "$MUNINN_DB_NAME" -U "$MUNINN_DB_READONLY_USER" \
-qtc "select rolpassword from pg_catalog.pg_authid where rolname = '${MUNINN_DB_READONLY_USER}'" \
| xargs \
)
SUHASH=$( \
psql -h "$MUNINN_DB_HOST" -p "$MUNINN_DB_PORT" -d "$MUNINN_DB_NAME" -U "$MUNINN_DB_READONLY_USER" \
-qtc "select rolpassword from pg_catalog.pg_authid where rolname = '${MUNINN_DB_SUPERUSER}'" \
| xargs \
)
{
  printf '"%s" "%s"\n' "${MUNINN_DB_READONLY_USER}" "${ROHASH}";
  printf '"%s" "%s"\n' "${MUNINN_DB_SUPERUSER}" "${SUHASH}";
} > /etc/pgbouncer/userlist.txt
chmod 600 /etc/pgbouncer/userlist.txt

# pgbouncer.ini setup
INI="/etc/pgbouncer/pgbouncer.ini"
:> "$INI"
{
  echo "[databases]";
  echo "${MUNINN_DB_NAME} = host=${MUNINN_DB_HOST} port=${MUNINN_DB_PORT} dbname=${MUNINN_DB_NAME}";
  echo
  echo "[pgbouncer]";
  echo "listen_addr=0.0.0.0";
  echo "listen_port=6432";
  echo "auth_type=scram-sha-256";
  echo "auth_file=/etc/pgbouncer/userlist.txt"
  echo
  echo "pool_mode=transaction";
  echo "max_client_conn=1000"; #todo
  echo "server_idle_timeout=300";
  echo ""
} >> "$INI"

exec pgbouncer /etc/pgbouncer/pgbouncer.ini