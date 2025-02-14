# Bird Flu Database


## Testing Setup (containerized)

1. Clone repository
2. Create `.env` file
3. `source db_setup.sh`
    - We're going to skip over creating a less-privileged user for now.
4. Install python dependencies
    1. `virtualenv venv`
    2. `source venv/bin/activate`
    3. `pip install -r requirements.txt` 

## James's To Do List:

### Schema Updates

- ~~Make changes to add alleles table and change relationships~~
- Figure out what data we're using to identify alleles.
- Consider splitting off the AA part of the alleles

### Python Work

- ~~Update the nucleotides enum to allow for all the iupac values~~
- Drop the enum for region, it can just be text
  - I think we acually want to revisit this. I think it might be a lot faster to select by an integer than by a string.
- ~~Figure out how to automate the IntEnums in alembic migrations~~
- Look into extensions to alembic
- 
### Deployment Stuff

- Get pg 16 installed on kenny
- Create basic .conf files for local access on unix sockets
- Figure out how to make deployment and config less manual
- Decide on resource allocation to postgres
- Update postgres memory settings, esp. shared_buffers, maybe work_mem.
- Set up pg stat statements as part of deployment.

### Misc

- Decide if we're doing backups
- 