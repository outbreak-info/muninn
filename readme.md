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
    export FLU_DB_PORT="5430"
    ```
    - On kenny, you may need to mess with the port to avoid conflicting with the container I have running.
      Changing the setting in .env will cascade to everywhere else, so just change it there.
    - For now, 'flu' will be the postgres superuser and own everything, eventually we'll want to have less privileged
      roles.
3. `source .env`
4. `source db_setup.sh`
    - Note that I've gotten rid of the docker volume, so if you delete the container, you'll have to set up the db from
      scratch again.
5. Create virtualenv and install python dependencies
    1. `virtualenv venv`
    2. `source venv/bin/activate`
    3. `pip install -r requirements.txt`
6. Run the alembic migrations: `alembic upgrade head`. (This requires .env values, make sure they're still set.)
7. Obtain the archive of test data from James and unpack it. You can do this wherever you want, but I recommend in the
   project root directory.
    1. `cd /wherever/you/put/it/bird_flu_db`
    2. `mkdir test_data`
    3. `cd test_data`
    4. `cp /wherever/flu_db_test_data.tar.gz .`
    5. `tar -xzvf flu_db_test_data.tar.gz`
8. Run the data insertion script.
    1. Get back to the project root and make sure you still have the .env values set
    2. Run the script, providing it the absolute path to the test data directory:
        - `python3 runinserts.py /wherever/you/put/it/bird_flu_db/test_data`
        - This will take a while, like half an hour.
9. If you want, run the web server and try out a request. This part is still very much WiP.
    1. `fastapi dev api/main.py` This will start up the server and restart it whenever the sources change.
    2. There's only one endpoint even marginally complete at the moment, so play with it:
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
For example, `/variants/by/sample/collection_start_date >= 2024-01-01 & host = cat` will result in the following SQL
being used in the query above:
`SELECT samples.id FROM samples WHERE collection_start_date >= '2024-01-01' & host = 'cat'`.

Here's a quick (and quite possibly outdated) guide to the available syntax:
You can use equivalence relations: `=, !=, >, <, <=, >=`, boolean operators `&, |, !`, and parentheses to group terms.
Greater than and less than are only usable with numeric or date values.
Dates must be entered in the format `\d{4}-\d{2}-\d{2}`.
In text inputs, only letters, numbers, hyphens and underscores are allowed.
Numbers may contain decimal points.

The only endpoint live allows you to select variants based on sample, so the user-defined part of the query is against
the samples table. Here's a summary of the fields in that table:

| Column Name              | Type                     |
|--------------------------|--------------------------|
| id                       | bigint                   |                   
| accession                | text                     |                     
| consent_level            | text                     |                     
| bio_project              | text                     |                     
| bio_sample               | text                     |                     
| bio_sample_accession     | text                     |                     
| bio_sample_model         | text                     |                     
| center_name              | text                     |                     
| experiment               | text                     |                     
| host                     | text                     |                     
| instrument               | text                     |                     
| platform                 | text                     |                     
| isolate                  | text                     |                     
| library_name             | text                     |                     
| library_layout           | text                     |                     
| library_selection        | text                     |                     
| library_source           | text                     |                     
| organism                 | text                     |                     
| is_retracted             | boolean                  |                  
| retraction_detected_date | timestamp with time zone | 
| isolation_source         | text                     |                     
| release_date             | timestamp with time zone | 
| creation_date            | timestamp with time zone | 
| version                  | text                     |                     
| sample_name              | text                     |                     
| sra_study                | text                     |                     
| serotype                 | text                     |                     
| assay_type               | text                     |                     
| avg_spot_length          | double precision         |         
| bases                    | bigint                   |                   
| bytes                    | bigint                   |                   
| datastore_filetype       | text                     |                     
| datastore_region         | text                     |                     
| datastore_provider       | text                     |                     
| geo_location_id          | bigint                   |                   
| collection_start_date    | date                     |                     
| collection_end_date      | date                     |     


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

## Misc / Useless Info:

### Installing a python 3.10 for testing

**Never Mind...**
This led to problems with the pycharm debugger that do not seem worth solving.
I'll keep the instructions in case I need them again sometime.

Here are the steps I took to allow testing against python 3.10 on my system, which shipped with a newer version.

1. Install pyenv as indicated in their [github](https://github.com/pyenv/pyenv)
2. Make sure that requirements.txt is up to date, then delete any existing venv for this project.
3. Install 3.10: `pyenv install 3.10`
4. Make 3.10 the default within the project directory:
    1. `cd /.../bird_flu_db`
    2. `pyenv local 3.10`
5. Make sure that the correct version of python is in use: `python3 --version` should respond `Python 3.10.16`
6. Recreate the virtual environment. This is a minefield of options...
    1. `python3 -m venv venv`
7. `source venv/bin/activate`
8. Install dependencies: `pip install -r requirements.txt`
9. I use Pycharm. Pycharm will need some help to figure out what's going on.
    1. Open settings, go to `Project: bird_flu_db > Python Interpreter`
    2. Click `Add Interpreter > add local interpreter`, and switch to `Select existing`.
    3. It may find the python 3.10 interpreter, but that won't be good enough because that interpreter is installed
       centrally, while the packages are installed in the venv. Click the little folder to browse your filesystem.
    4. Select `bird_flu_db/venv/bin/python3.10`
    5. And hopefully that's it.