#!/bin/bash

export PGPASSWORD="$POSTGRES_PASSWORD"
NEW_PASS_HASH=$(echo "$FLU_DB_READONLY_PASSWORD" | python3 /muninn/scripts/scram.py)
psql -d flu -U flu \
-c "CREATE USER $FLU_DB_READONLY_USER WITH PASSWORD '$NEW_PASS_HASH';" \
-c "GRANT pg_read_all_data TO $FLU_DB_READONLY_USER;"

unset PGPASSWORD