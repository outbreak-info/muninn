FROM postgres:16-bullseye

RUN apt-get update && apt-get install -y python3

COPY containers/database/bin/* /muninn/bin/