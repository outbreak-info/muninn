# Bird Flu Database

Database system to store mutation and variant data for avian influenza.

## Testing Setup (containerized)

1. Clone repository and cd into it.
2. Create `.env` file.
    ```
    export FLU_DB_PASSWORD="default-flu"
    export FLU_DB_SUPERUSER_PASSWORD="default-superuser"
    
    export FLU_DB_USER="flu"
    export FLU_DB_HOST="localhost"
    export FLU_DB_DB_NAME="flu"
    export FLU_DB_PORT="5432"
    ```
    - On kenny, you may need to mess with the port to avoid conflicting with the container I have running.
      Changing the setting in .env will cascade to everywhere else, so just change it there.
    - For now, 'flu' will be the postgres superuser and own everything, eventually we'll want to have less privileged
      roles.
3. Run docker compose to start the database and api containers.
   This will start up two containers, one for postgres, and one for the webserver. The webserver container will
   insert test data into the database, which will take about an hour.
    1. `docker-compose -f docker-compose.yml up -d --build`
    2. Use `docker logs flu_db_server` to check on the progress of the insertion. (See below for more info.)
4. Once the data is loaded, the webserver will start and you can try out a request.
   There's only one endpoint even marginally complete at the moment, so play with it:
    - http://localhost:8000/variants/by/sample/accession=SRR28752446

The database is kept in a docker container called `flu_db_pg`.
To run psql (the postgres console) on the container, use the following command:

```
docker exec -it flu_db_pg psql -U flu -d flu -h localhost -p $FLU_DB_PORT
```

Or, if you have psql installed on the system hosting the container:

```
psql -U flu -d flu -h localhost -p $FLU_DB_PORT
```

**Note:**
Any time there's a change in the database schema, you'll have to recreate the database.
You can either drop the container entirely and rerun all the database setup steps, or use psql to truncate all the
tables, then run the migrations and repopulate the db.

psql allows you to run arbitrary sql statements, as well as some utility commands.
Here's some psql commands that are useful in general:

| Command                              | Use                                         |
|--------------------------------------|---------------------------------------------|
| `\?`                                 | Open help page                              |
| `\dt+`                               | List all the tables and their sizes on disk |
| `\d+ <table name>`                   | Describe the columns in a table             |
| `select count(*) from <table name>;` | Count the entries in a table                |
| `truncate table <name> cascade;`     | Delete all rows from a table                |

While waiting on the insertions, use `\dt+` to view the sizes of the tables on disk.
Most of the insertion time is spent loading variants and mutations.
There are slightly over 200k of each.
You can check the number of rows in those tables to get a sense of how much work is left.

### Recreate Server Container without Changing Database

If you recreate the database container, it takes a long time to reload all the data.
Here's how you recreate just the server container without taking down the database.

1. `docker stop flu_db_server`
2. `docker rm flu_db_server`
3. `docker-compose -f docker-compose.yml up -d --no-deps --no-recreate --build server`

I've only tested these instructions using podman on my machine, so they may fail you.

## Query Syntax

The endpoints (which currently means just `/variants/by/sample`) use a restricted query syntax to allow the user to
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

The following endpoints are currently live:

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

Note: using `id` as a field in any query (e.g.: `id = 1234`) is likely to fail.
This is because multiple tables, each with their own `id` column are joined before being queried, and SQL will not allow
a query to use an ambiguous column name.
As far as I know, the only columns affected by this are ids which we are unlikely to want to use in queries anyway, so 
fixing this will not be a priority unless a use-case arises.

Have fun!

## Notes

### Geo Locations

I want to know about the format in which `geo_loc_name` and its ilk are sent to me.
[This link](https://www.ncbi.nlm.nih.gov/sra/docs/sra-cloud-based-metadata-table/) gives a brief overview of the
metadata fields available for samples in the SRA, but it doesn't give any details about the formatting of those pesky
location strings.
[Here](https://www.ncbi.nlm.nih.gov/biosample/docs/attributes/) we get some more details about the formatting
(Ctrl-F for "geo_loc_name").
And that entry will point us to
[this page from the INSDC](https://www.insdc.org/submitting-standards/geo_loc_name-qualifier-vocabulary/),
which specifies the format for these country strings and gives the list of allowed country names.

According to the INSDC page, the format is `<geo_loc_name>[:<region>][, <locality>]`, where `<geo_loc_name>` is the name
of either a country or an ocean, from the approved list given on the page.
They don't give much detail for the region and locality stuff, and worse, some of our examples don't follow this format.
Entries like `USA: Plympton, MA` go `<geo_loc_name>:<locality>, <region>`.
But since these entries seem to always use a state postal code, we should be able to make a functional parsing system
based on this.
