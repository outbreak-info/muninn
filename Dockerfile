FROM postgres:latest
WORKDIR /

COPY *.sql /docker-entrypoint-initdb.d/
# ENV POSTGRES_PASSWORD='h5n1'
ENV POSTGRES_HOST_AUTH_METHOD='trust'
EXPOSE 5432

