#!/bin/bash

export PGPASSWORD="$POSTGRES_PASSWORD"
NEW_PASS_HASH=$(echo "$MUNINN_DB_READONLY_PASSWORD" | python3 /muninn/bin/scram.py)
psql -d flu -U flu \
-c "CREATE USER $MUNINN_DB_READONLY_USER WITH PASSWORD '$NEW_PASS_HASH';" \
-c "GRANT pg_read_all_data TO $MUNINN_DB_READONLY_USER;"

unset PGPASSWORD