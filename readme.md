# Muninn --- Viral Mutation Database

Database system to store mutation and variant data for avian influenza.

## Testing Setup (containerized)

1. Clone repository and cd into it.
2. Create `.env` file.
    ```
    export FLU_DB_READONLY_USER="flu_reader"
    export FLU_DB_READONLY_PASSWORD="default-flu-reader"

    export FLU_DB_SUPERUSER_PASSWORD="default-flu"
    export FLU_DB_SUPERUSER="flu"

    export FLU_DB_HOST="localhost"
    export FLU_DB_DB_NAME="flu"
    export FLU_DB_PORT="5432"
    
   # this will be mounted to the server container as /flu/data
    export FLU_DB_SERVER_DATA_INPUT_DIR="/dev/null"
   
   # this controls which config file is applied to postgres
    export PG_CONFIG_NAME="local"
    ```
    - On kenny, you may need to mess with the port to avoid conflicting with the container I have running.
      Changing the setting in .env will cascade to everywhere else, so just change it there.
    - For now, 'flu' will be the postgres superuser and own everything, eventually we'll want to have less privileged
      roles.
    - Change the value for `FLU_DB_SERVER_DATA_INPUT_DIR` to allow the server to read input data from a host directory.
3. Run docker compose to start the database and api containers.
    1. `docker-compose -f docker-compose.yml up -d --build`
    2. This will start up two containers, `flu_db_pg` for postgres, and `flu_db_server` for the webserver.
    3. The server container will automatically start fastAPI.
    4. Use `docker logs flu_db_server` to see server logs.
4. Update the database schema: `docker exec -d flu_db_server muninn_schema_update`
5. Load or update data:  `docker exec -d flu_db_server muninn_ingest_all`
    1. Input data must be placed in `FLU_DB_SERVER_DATA_INPUT_DIR` on the host machine.
       For details read ingestion script: `containers/server/bin/muninn_ingest_all`
    2. This process will take 15-45 minutes to finish, but existing records will be updated in-place, and the webserver
       will remain available.
    3. For information on logs see Troubleshooting Information > Webserver
6. Load or update test data: `docker exec -d flu_db_server muninn_ingest_playset ${FLU_DB_SERVER_DATA_INPUT_DIR}/<archive name>`
    1. Input data must be placed in `FLU_DB_SERVER_DATA_INPUT_DIR` on the host machine.
       For details read ingestion script: `containers/server/bin/muninn_ingest_playset`
    2. This process will take a few minutes and data will persist in a docker volume. Please see `docker-compoase.yml` for details.
    3. For information on logs see Troubleshooting Information > Webserver

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

- `/variants/freqency?aa=HA:Q238R`
    - Also allows queries based on nucleotide with parameter `nt=HA:A123C`
- `/mutations/frequency?aa=HA:Q238R`
    - Also allows queries based on nucleotide as above.
    - Returns count of samples associated with the given amino acid or nucleotide change.
- `/variants/frequency/score?region=HA&metric=stablility`

Note: using `id` as a field in any query (e.g.: `id = 1234`) is likely to fail.
This is because multiple tables, each with their own `id` column are joined before being queried, and SQL will not allow
a query to use an ambiguous column name.
As far as I know, the only columns affected by this are ids which we are unlikely to want to use in queries anyway, so
fixing this will not be a priority unless a use-case arises.

Have fun!
