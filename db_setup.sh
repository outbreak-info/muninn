#!/usr/bin/env bash

source .env

# todo: these are temporary for testing with containers
podman build -q --tag 'flu_db' .
podman run -qd -e POSTGRES_PASSWORD="$FLU_DB_SUPERUSER_PASSWORD" -p 127.0.0.1:5432:5432 flu_db
sleep 10

export PGPASSWORD=$FLU_DB_SUPERUSER_PASSWORD

psql -U postgres -h 127.0.0.1 -p 5432 -c "create user flu password '$FLU_DB_PASSWORD';"
psql -U postgres -h 127.0.0.1 -p 5432 -c "create database flu owner flu;"

#export PGPASSWORD=$FLU_DB_PASSWORD
#psql -U flu -h 127.0.0.1 -p 5432 -f create_tables.sql
#
#unset PGPASSWORD