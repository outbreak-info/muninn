#!/bin/bash

export MUNINN_DB_HOST=localhost
export MUNINN_DB_SUPERUSER="flu"
export MUNINN_DB_READONLY_USER="flu_reader"
export MUNINN_DB_READONLY_PASSWORD=default-flu-reader
export MUNINN_DB_SUPERUSER_PASSWORD=default-flu
export MUNINN_DB_PORT=5436
export MUNINN_DB_NAME="flu"
export MUNINN_SERVER_DATA_INPUT_DIR="/home/yutianc/muninn/inputs/sd_all"

fastapi dev api/main.py --port 8003 --host 0.0.0.0