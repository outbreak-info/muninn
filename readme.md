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


### Installing a python 3.10 for testing

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
   


## Notes

### Geo Locations

I want to know about the format in which `geo_loc_name` and its ilk are sent to me.
[This link](https://www.ncbi.nlm.nih.gov/sra/docs/sra-cloud-based-metadata-table/) gives a  brief overview of the 
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

