#!/bin/bash
set -euo pipefail

CHUNKED_MUT_FILES_DIR=/home/yutianc/bjorn/mutations/gb_all_skip_missing/chunks
MUNINN_BASE_INPUT_DIR=/home/yutianc/muninn/inputs/sc2_all
CONTAINER_NAME=sc2_all_server
ENV_FILE=.env
TOTAL_CHUNKS=5

LINEAGES_FILE="${MUNINN_BASE_INPUT_DIR}/lineage_gb.csv"
LINEAGES_HIERARCHY_FILE="${MUNINN_BASE_INPUT_DIR}/lineages.yml"
METADATA_FILE="${MUNINN_BASE_INPUT_DIR}/metadata_gb.tsv"
VARIANTS_FILE="${MUNINN_BASE_INPUT_DIR}/variants_dummy.tsv"
UNIQUE_TO_IDS_FILE="${MUNINN_BASE_INPUT_DIR}/unique_to_dups.txt"
DMS_FILE="${MUNINN_BASE_INPUT_DIR}/dms_all_processed_BA.1_BA.2_Hu-1.tsv"
EVESCAPE_FILE="${MUNINN_BASE_INPUT_DIR}/evescape_processed.csv"

START_FROM=$1

FIRST_CHUNK="01"
LAST_CHUNK=$(printf "%02d" "$TOTAL_CHUNKS")

LOG="$PWD/ingest_chunks.log"
exec >> "$LOG" 2>&1

for i in $(seq -f "%02g" "$START_FROM" "$TOTAL_CHUNKS"); do
    # make the current input dir and make it executeable
    MUNINN_CHUNK_INPUT_DIR="${MUNINN_BASE_INPUT_DIR}/chunk_${i}"
    mkdir -p "$MUNINN_CHUNK_INPUT_DIR"
    chmod 777 "$MUNINN_CHUNK_INPUT_DIR"

    # update the input dir to the chunk input dir in .env file
    sed -i "s|^export MUNINN_SERVER_DATA_INPUT_DIR=.*|export MUNINN_SERVER_DATA_INPUT_DIR=\"${MUNINN_CHUNK_INPUT_DIR}\"|" "$ENV_FILE"
    source "$ENV_FILE"

    if [[ "$i" == "$FIRST_CHUNK" ]]; then
        # first chunk: ingest samples, lineages, hierarchy, and mutations
        FILES_TO_ZIP=(
            "${MUNINN_CHUNK_INPUT_DIR}/mutations_chunk_${i}.tsv"
            "$LINEAGES_FILE"
            "$LINEAGES_HIERARCHY_FILE"
            "$METADATA_FILE"
            "$VARIANTS_FILE"
            "$UNIQUE_TO_IDS_FILE"
        )
    elif [[ "$i" == "$LAST_CHUNK" ]]; then
        # last chunk: ingest mutations + pheno metrics
        FILES_TO_ZIP=(
            "${MUNINN_CHUNK_INPUT_DIR}/mutations_chunk_${i}.tsv"
            "$VARIANTS_FILE"
            "$DMS_FILE"
            "$EVESCAPE_FILE"
        )
    else
        # middle chunks: mutations only
        FILES_TO_ZIP=(
            "${MUNINN_CHUNK_INPUT_DIR}/mutations_chunk_${i}.tsv"
            "$VARIANTS_FILE"
        )
    fi


    if [[ ! -f "${MUNINN_CHUNK_INPUT_DIR}/chunk_${i}.zip" ]]; then
        CHUNK_FILE="${CHUNKED_MUT_FILES_DIR}/mutations_chunk_${i}.tsv.gz"
        if [[ ! -f "${MUNINN_CHUNK_INPUT_DIR}/mutations_chunk_${i}.tsv" ]]; then
            gunzip -c "$CHUNK_FILE" > "${MUNINN_CHUNK_INPUT_DIR}/mutations_chunk_${i}.tsv"
        fi

        # check if the files exist
        for f in "${FILES_TO_ZIP[@]}"; do
            [[ -f "$f" ]] || { echo "ERROR: File not found: $f"; exit 1; }
        done
        zip -j "${MUNINN_CHUNK_INPUT_DIR}/chunk_${i}.zip" "${FILES_TO_ZIP[@]}"
    fi



    # bring down any running instance so the new volume mount takes effect
    docker compose -f docker-compose.yml down || true

    if [[ "$i" == "$FIRST_CHUNK" ]]; then
        docker compose -f docker-compose.yml up -d --build
    else
        docker compose -f docker-compose.yml up -d # need this since we update the .env
    fi
    sleep 60

    if [[ "$i" == "$FIRST_CHUNK" ]]; then
        docker exec "$CONTAINER_NAME" muninn_schema_update
        sleep 30
    fi

    docker exec -it "$CONTAINER_NAME" muninn_ingest_sc2 --auto --archive_in "chunk_${i}.zip"

    # rm the unzipped mut files to clean up disk space
    rm "${MUNINN_CHUNK_INPUT_DIR}/mutations_chunk_${i}.tsv"

done

# to run this: ./ingest_chunks.sh 01