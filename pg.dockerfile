FROM postgres:18-bookworm

RUN apt-get update && \
	apt-get install -y python3 postgresql-18-roaringbitmap vim && \
	rm -rf /var/lib/apt/lists/*

COPY containers/database/bin/* /muninn/bin/