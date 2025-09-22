# Muninn --- Viral Mutation Database

Database system to store mutation and variant data for avian influenza.

## Testing Setup (containerized)

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
    
    # this will be mounted to the server container as /flu/data
    export MUNINN_SERVER_DATA_INPUT_DIR="/dev/null"
    
    # this controls which config file is applied to postgres
    export MUNINN_PG_CONFIG_NAME="local"
    
    # this will be used as a prefix to the container names
    export MUNINN_INSTANCE_NAME="flu_db"
    ```
    - Change the value for `MUNINN_DB_SERVER_DATA_INPUT_DIR` to allow the server to read input data from a host directory.
    - If the server and DB are running on the same host, they will talk through the docker network. 
    In that case, `MUNINN_DB_PORT_FOR_SERVER` should be 5432, regardless of the value of `MUNINN_DB_PORT`, 
    and `MUNINN_DB_HOST` should be `"postgres"`, which is the name of the database service within docker.
    - If the DB and server are on different hosts, then `MUNINN_DB_HOST` should be the DB host, and `MUNINN_DB_PORT_FOR_SERVER` must be the same as `MUNINN_DB_PORT`
3. Run docker compose to start the database and api containers.
    1. `docker-compose -f docker-compose.yml up -d --build`
    2. This will start up two containers, `flu_db_pg` for postgres, and `flu_db_server` for the webserver.
    3. The server container will automatically start fastAPI.
    4. Use `docker logs flu_db_server` to see server logs.
4. Update the database schema: `docker exec -d flu_db_server muninn_schema_update`
5. Load or update data:  `docker exec -d flu_db_server muninn_ingest_all`
    1. Input data must be placed in `MUNINN_DB_SERVER_DATA_INPUT_DIR` on the host machine.
       For details read ingestion script: `containers/server/bin/muninn_ingest_all`
    2. This process will take 15-45 minutes to finish, but existing records will be updated in-place, and the webserver
       will remain available.
    3. For information on logs see Troubleshooting Information > Webserver
6. Load or update test data: `docker exec -d flu_db_server muninn_ingest_playset ${MUNINN_DB_SERVER_DATA_INPUT_DIR}/<archive name>`
    1. Input data must be placed in `MUNINN_DB_SERVER_DATA_INPUT_DIR` on the host machine.
       For details read ingestion script: `containers/server/bin/muninn_ingest_playset`
    2. This process will take a few minutes and data will persist in a docker volume. Please see `docker-compose.yml` for details.
    3. For information on logs see Troubleshooting Information > Webserver

### Running Multiple Instances

In some cases we want to run multiple instances of Muninn on one host. 
For most use-cases this shouldn't be required, so these notes are largely for my own use and might not be complete.

The motivation here is using an instance of the server on host A to do the data ingestion for a database on host B. 
We have a few reasons for wanting to do it this way, but primarily, we want to avoid needing to copy the input data over.
This way we avoid that issue by reusing the ingestion machinery we already have in place, and simply pointing it at a different target.

It's best to have two copies of the project, one for each instance.
By default, docker's `project-name` will be set using the name of the root directory of the project.
If you run multiple instances from a single copy, then you'll have to explicitly set `project-name`:
```commandline
docker compose --project-name $MUNINN_INSTANCE_NAME up -d --build
docker compose --project-name $MUNINN_INSTANCE_NAME down -v
```
Just make a second copy of the repo, it's the easier way.
Beyond this point, these instructions will assume separate directories.

In the `.env` file for your new instance: 
1. Change the `MUNINN_INSTANCE_NAME` to something other than the default.
2. Change the server and database ports.
3. Ideally, change `MUNINN_SERVER_DATA_INPUT_DIR`. 
Allowing multiple instances to share the input directory shouldn't break anything, but it introduces opportunities for bugs and confusion.

Recall our goal: to use a server instance on host A to manage data ingestion for a database on host B. 
Obviously what we want, then, is to run only the server, and not the database: 
```commandline
docker compose -f docker-compose.yml up -d --no-deps --build server
```

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

Here's a quick (and quite possibly outdated) guide to the available syntax:
You can use equivalence relations: `=, !=, >, <, <=, >=`.
Greater than and less than are only usable with numeric or date values.
The available boolean operators are: `^`, `|`, and `!`, meaning `and`, `or` and `not`, respectively.
Parentheses may be used to group terms, e.g.: `(host = cat | host = dog) ^ region_name = Minnesota`.
Dates must be entered in the format `\d{4}-\d{2}-\d{2}`.
In text inputs, only letters, numbers, hyphens and underscores are allowed.
Numbers may contain decimal points.

## Endpoints

**I'm no longer updating this list to include new endpoints.**
Auto-generated documentation for the API can be found at `<host>:8000/docs`.

The following endpoints are currently live:

Get item by id:

- `/sample/{id}`

These endpoints simply allow you to query a particular collection:

- `/samples?q=<query>`
- `/variants?q=<query>`
- `/mutations?q=<query>`

These allow you to query one collection based on properties of related entries in other collections:

- `/variants/by/sample?q=<query>`
- `/mutations/by/sample?q=<query>`
- `/samples/by/mutation?q=<query>`
- `/samples/by/variant?q=<query>`

Simple counts:

- `/count/{x}/by/{y}`
    - `{x}` is one of `samples`, `variants`, or `mutations`
    - `{y}` is the name of a column from `{x}`. In the case of `variants` and `mutations`, columns from `alleles`
      and `amino_acid_substitutions` are also allowed.

Prevalence:

- `/variants/frequency?aa=HA:Q238R`
    - Also allows queries based on nucleotide with parameter `nt=HA:A123C`
- `/mutations/frequency?aa=HA:Q238R`
    - Also allows queries based on nucleotide as above.
    - Returns count of samples associated with the given amino acid or nucleotide change.
- `/variants/frequency/score?region=HA&metric=stability`

Note: using `id` as a field in any query (e.g.: `id = 1234`) is likely to fail.
This is because multiple tables, each with their own `id` column are joined before being queried, and SQL will not allow
a query to use an ambiguous column name.
As far as I know, the only columns affected by this are ids which we are unlikely to want to use in queries anyway, so
fixing this will not be a priority unless a use-case arises.

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

Lineages are allowed to form a DAG, and a `BEFORE INSERT` trigger prevents any cycle-producing entries from being added to `lineages_immediate_children`.


Have fun!
