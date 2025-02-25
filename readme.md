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
        - http://localhost:8000/variants/by/sample/?accession=SRR28752446