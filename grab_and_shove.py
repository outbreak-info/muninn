import requests
import json
import sqlalchemy

BASE_URL = "http://kenny.scripps.edu:9200"

# start with 1000 metadatas?

r = requests.get(f'{BASE_URL}/metadata/_search?q=*&size=1000')

metadatas = [md['_source'] for md in json.loads(r.text)['hits']['hits']]

metadata_field_conversion = {
    "Assay Type": "assay_type",
    "AvgSpotLen": "avg_spot_len",
    "Bases": "bases",
    "BioProject": "bio_project",
    "BioSample": "bio_sample",
    "BioSample Accession": "bio_sample_accession",
    "BioSampleModel": "bio_sample_model",
    "Bytes": "bytes",
    "Center Name": "center_name",
    "Collection_Date": "collection_date",
    "Consent": "consent",
    "DATASTORE filetype": "datastore_filetype",
    "DATASTORE provider": "datastore_provider",
    "DATASTORE region": "datastore_region",
    "Experiment": "experiment",
    "Host": "host",
    "Instrument": "instrument",
    "Library Name": "library_name",
    "LibraryLayout": "library_layout",
    "LibrarySelection": "library_selection",
    "LibrarySource": "library_source",
    "Organism": "organism",
    "Platform": "platform",
    "ReleaseDate": "release_date",
    "Run": "run",
    "SRA Study": "sra_study",
    "Sample Name": "sample_name",
    "create_date": "create_date",
    "geo_loc_name": "geo_loc_name",
    "geo_loc_name_country": "geo_loc_name_country",
    "geo_loc_name_country_continent": "geo_loc_name_country_continent",
    "isolate": "isolate",
    "isolation_source": "isolation_source",
    "serotype": "serotype",
    "version": "version"
}

mds_translated = [{v: md[k] for k, v in metadata_field_conversion.items()} for md in metadatas]
# mds_translated = mds_translated[0:1] #todo rm
# fix collection date formats
for md in mds_translated:
    if md['collection_date'] in {'2023', '2024'}:
        md['collection_date'] = f'{md['collection_date']}-1-1'

def create_pg_engine():
    db_name = "postgres"
    db_user = "postgres"
    db_password = ""
    db_host = "localhost"
    db_port = 5432
    return sqlalchemy.create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

engine = create_pg_engine()

metadata_table = sqlalchemy.Table('metadata', sqlalchemy.MetaData(), autoload_with=engine)

with engine.connect() as conn:
    conn.execute(sqlalchemy.insert(metadata_table), mds_translated)

sras = [md['run'] for md in mds_translated]

print('metadata done')

#### Now do mutations ####

# get the db ids for each sra
with engine.connect() as conn:
    sras_formatted = ','.join([f"'{sra}'" for sra in sras])
    r = conn.execute(sqlalchemy.text(f'select run, id from metadata where run in ({sras_formatted});'))
    sra_to_id = {i['run']: i['id'] for i in r.mappings().all()}

# get mutations data
mutations = []
for sra in sras:
    r = requests.get(f'{BASE_URL}/mutations/_search?q=sra:{sra}&size=1000')
    mutations += [mut['_source'] for mut in json.loads(r.text)['hits']['hits']]

mutations = [m for m in mutations if m['sra'] in sras]

# shim values not in es
for m in mutations:
    m['position_nt'] = 0
    m['ref_nt'] = 'N'
    m['alt_nt'] = 'N'
    m['metadata_id'] = sra_to_id[m.pop('sra')]

mutations_field_conversion = {
    "alt": "alt_aa",
    "pos": "position_aa",
    "region": "region",
    'ref': 'ref_aa',
    'alt': 'alt_aa',
    'position_nt': 'position_nt',
    'ref_nt': 'ref_nt',
    'alt_nt': 'alt_nt',
    'metadata_id': 'metadata_id'
}

mutations_translated = [{v: m[k] for k, v in mutations_field_conversion.items()} for m in mutations]

# mutations_translated = mutations_translated[0:3] #rm

mutations_table = sqlalchemy.Table('mutations', sqlalchemy.MetaData(), autoload_with=engine)

with engine.connect() as conn:
    conn.execute(sqlalchemy.insert(mutations_table), mutations_translated)

print('mutations done')