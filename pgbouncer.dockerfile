FROM debian:bookworm

RUN \
apt-get update && \
apt-get install -y pgbouncer vim less && \
/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y && \
apt-get install -y postgresql-client-18 && \
rm -rf /var/lib/apt/lists/*

COPY containers/bouncer/entrypoint.sh /

USER postgres