#!/usr/bin/env bash

source .env

docker-compose -f docker-compose.yml up -d


while [ "$(docker inspect -f "{{.State.Health.Status}}" "flu_db_pg")" != "healthy" ]; do
  docker inspect -f "{{.State.Health.Status}}" "flu_db_pg"
  sleep 1
done

# for later use when not running in docker
while ! pg_isready -p "$FLU_DB_PORT" -U flu -h localhost; do
  echo 'wait'
  sleep 1
done

# and we're done here. The app startup will handle running the migration to