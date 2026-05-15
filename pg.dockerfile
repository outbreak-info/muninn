FROM postgres:18-bookworm

RUN apt-get update && apt-get install -y python3

COPY containers/database/bin/* /muninn/bin/