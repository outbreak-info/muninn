# Muninn

Muninn is a database system designed to store consensus and intra-host mutation data for avian influenza and SARS-CoV-2.

## Containerized Setup

1. Clone repository and cd into it.
2. Create `.env` file.
    ```shell
    export MUNINN_DB_READONLY_USER="flu_reader"
    export MUNINN_DB_READONLY_PASSWORD="default-flu-reader"
    export MUNINN_DB_SUPERUSER="flu"
    export MUNINN_DB_SUPERUSER_PASSWORD="default-flu"
    export MUNINN_DB_NAME="flu"
    
    # Use "postgres" when running on same host (linked via docker network)
    export MUNINN_DB_HOST="postgres"
    export MUNINN_DB_PORT="5432"
    # If running on the same host (and using docker networking) this should be 5432 regardless of the value of MUNINN_DB_PORT
    export MUNINN_DB_PORT_FOR_SERVER="5432"
   
    export MUNINN_SERVER_PORT="8000"
    
    # this will be mounted to the server container as /home/muninn/data
    export MUNINN_SERVER_DATA_INPUT_DIR="/dev/null"
    
    # this controls which config file is applied to postgres
    export MUNINN_PG_CONFIG_NAME="local"
    
    # this will be used as a prefix to the container names
    export MUNINN_INSTANCE_NAME="flu_db"
   
    # this is not used in the default docker-compose file
    # directory to be mounted to store postgres data
    export MUNINN_PG_DATA_BIND_DIR="/dev/null"
    ```
    - Change the value for `MUNINN_SERVER_DATA_INPUT_DIR` to allow the server to read input data from a host directory.
    - If the server and DB are running on the same host, they will talk through the docker network. 
    In that case, `MUNINN_DB_PORT_FOR_SERVER` should be 5432, regardless of the value of `MUNINN_DB_PORT`, 
    and `MUNINN_DB_HOST` should be `"postgres"`, which is the name of the database service within docker.
    - If the DB and server are on different hosts, then `MUNINN_DB_HOST` should be the DB host, and `MUNINN_DB_PORT_FOR_SERVER` must be the same as `MUNINN_DB_PORT`
    - For local testing, `MUNINN_PG_DATA_BIND_DIR` does not need to be set. 
3. Run docker compose to start the database and api containers.
    1. `docker-compose -f docker-compose.yml up -d --build`
    2. This will start up two containers, `flu_db_pg` for postgres, and `flu_db_server` for the webserver.
    3. The server container will automatically start fastAPI.
    4. Use `docker logs flu_db_server` to see server logs.
4. Update the database schema: `docker exec -d flu_db_server muninn_schema_update`
5. Load or update data:  `docker exec -d flu_db_server muninn_ingest_all --auto --archive_in <name of archive>`
    1. Input data must be placed in `MUNINN_SERVER_DATA_INPUT_DIR` on the host machine, in either `.zip` or `.tar.gz` format.
           For details read ingestion script: `containers/server/bin/muninn_ingest_all`
    2. This process will take 15-45 minutes to finish, but existing records will be updated in-place, and the webserver
       will remain available.
    3. For information on logs see Troubleshooting Information > Webserver
    4. The `--auto` flag is optional, but this mode avoids the need to adhere to specific file and dir names within the input archive.

### Running Multiple Instances

In some cases we want to run multiple instances of Muninn on one host.

The `MUNINN_INSTANCE_NAME` is used to name docker containers, volumes, etc to avoid conflicts between multiple instances of Muninn.
If multiple instances share a host, each must have a unique value for this variable.

It is possible to run multiple instances of muninn using a single copy of the project, but in most cases it will be more natural to have one copy per instance.
Beyond this point, these instructions will assume the latter case.

In the `.env` file for your new instance: 
1. Change the `MUNINN_INSTANCE_NAME` to avoid conflicts with other instances on the host.  
   (Hint: use `docker ps` to see what else is running.)
2. Change `MUNINN_DB_PORT` and `MUNINN_SERVER_PORT` to avoid conflicts.  
   (No need to change `MUNINN_DB_PORT_FOR_SERVER`)
3. Ideally, change `MUNINN_SERVER_DATA_INPUT_DIR`. 
Allowing multiple instances to share the input directory shouldn't break anything, but it introduces opportunities for bugs and confusion.

### Postgres Data Location

The default `docker-compose.yml` creates an anonymous volume to hold Postgres data.
The location of this volume is decided by docker configuration on the host machine, but by default it will be stored on the root partition. 
When using the default docker compose file, `MUNINN_PG_DATA_BIND_DIR` is ingored, and changing it will have no effect.

The alternate compose file `docker-compose.bind-pg-data.yml` gives us the option to store Postgres data in a directory of our choosing.
This directory will be bound to the Postgres container. 
Set the directory using `MUNINN_PG_DATA_BIND_DIR`, and modify commands to use the alternate compose file: `docker-compose -f docker-compose.bind-pg-data.yml ...`
Data stored in a bound directory will not be cleared with `docker compose down -v`.

## Troubleshooting Tools

To recreate both docker containers run the following:
```
docker compose down
docker compose -f docker-compose.yml up -d --build
``` 
This will remove and rebuild both containers, but will not wipe out the contents of the database, which are maintained in a volume.

To wipe out the contents of the database as well, replace the first command with `docker compose down -v`.

### Database

To run `psql` (the postgres console) on the container, use the following command:

```
docker exec -it flu_db_pg psql -U flu -d flu -h localhost -p $FLU_DB_PORT
```

Or, if you have `psql` installed on the system hosting the container:

```
psql -U flu -d flu -h localhost -p $FLU_DB_PORT
```

`psql` allows you to run arbitrary SQL against the DB.
It's very useful for debugging.

### Webserver

To see the server (fastAPI) logs, run `docker logs flu_db_server`.

The logs for the database setup are kept at `/flu/db_setup.log`.
Logs from data ingestion scripts are date stamped like `ingest_all<date stamp>.log` and are stored in the mounted directory indicated by `FLU_DB_SERVER_DATA_INPUT_DIR`.

To get open a tty on the server container run `docker exec -it flu_db_server bash`

## Query Syntax

The endpoints use a restricted query syntax to allow the user to
control part of the query.
For example, in the case of `/variants/by/sample/`, here's how that works.
When you hit this endpoint, the api will always run the following query:

```sql
SELECT * FROM intra_host_variants ihv LEFT JOIN (
   alleles a LEFT JOIN amino_acid_substituions aas ON aas.allele_id = a.id
) ON ihv.allele_id = a.id
WHERE ihv.sample_id IN (
   SELECT samples.id FROM samples WHERE <user defined> 
);
```

Where `<user defined>` is filled in using the filters supplied by the user.
That is, you always select all the variants and alleles associated with a set of samples.
And the set of samples used to select those variants is based on a query that the user supplies.
For example, `/variants/by/sample/collection_start_date >= 2024-01-01 ^ host = cat` will result in the following SQL
being used in the query above:
`SELECT samples.id FROM samples WHERE collection_start_date >= '2024-01-01' ^ host = 'cat'`.

Here's a quick guide to the available syntax:
You can use equivalence relations: `=, !=, >, <, <=, >=`.
Greater than and less than are only usable with numeric or date values.
The available boolean operators are: `^`, `|`, and `!`, meaning `and`, `or` and `not`, respectively.
Parentheses may be used to group terms, e.g.: `(host = cat | host = dog) ^ admin1_name = Minnesota`.
Dates must be entered in the format `\d{4}-\d{2}-\d{2}`.
In text inputs, only letters, numbers, hyphens and underscores are allowed.
Numbers may contain decimal points.

Note: using `id` as a field in any query (e.g.: `id = 1234`) is likely to fail.
This is because multiple tables, each with their own `id` column are joined before being queried, and SQL will not allow a query to use an ambiguous column name.

## Endpoints

Auto-generated documentation for the API can be found at `<host>:8000/docs`.

In general, parameters called `q` are expected to use the query syntax outlined above.

## Lineage Hierarchy

The lineage hierarchy system allows us to store relationships between lineages in our database.
The `lineages_immedidate_children` table stores direct parent/child relationships. 
The parent and child must be from the same lineage system. 
Indirect relationships are accessed via a view: `lineages_deep_children`. 
This view is a recursive query against `lineages_immediate_children`, whose result is a table of all direct and indirect relationships between lineages.

For example, if we have the following entries in `lineages_immediate_children`:
```
parent  child
A       A.1
A.1     A.1.1
A.1     A.1.2
```
then `lineages_deep_children` will contain the following:
```
parent  child
A       A.1
A.1     A.1.1
A.1     A.1.2
A       A.1.1
A       A.1.2
```
(Lineage names are used here for simplicity. In the actual implementation, only IDs are used.)
Lineages are allowed to form a directed acyclic graph, and a `BEFORE INSERT` trigger prevents any cycle-producing entries from being added to `lineages_immediate_children`.


Have fun!
