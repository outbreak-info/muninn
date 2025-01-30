import requests
import json
import sqlalchemy
from sqlalchemy import insert
from sqlalchemy.orm import Session

import DB.engine
from DB.models import Metadata, Mutation

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

# fix collection date formats
for md in mds_translated:
    if md['collection_date'] in {'2023', '2024'}:
        md['collection_date'] = f'{md['collection_date']}-1-1'


engine = DB.engine.create_pg_engine()

with Session(engine) as session:
    res = session.scalars(
        insert(Metadata).returning(Metadata.id, sort_by_parameter_order=True),
        mds_translated
    )
    session.commit()
    metadata_ids_ordered = res.all()

print('metadata done')

# todo: have to check that the match is happening correctly
sra_to_id = dict(zip([md['run'] for md in mds_translated], metadata_ids_ordered))

#### Now do mutations ####

# get mutations data
mutations = []
for sra in sra_to_id.keys():
    r = requests.get(f'{BASE_URL}/mutations/_search?q=sra:{sra}&size=1000')
    mutations += [mut['_source'] for mut in json.loads(r.text)['hits']['hits']]

# we filter b/c this is search and there will always be results, even if the sra doesn't match
# todo: check up on metadatas with no mutations
mutations = [m for m in mutations if m['sra'] in sra_to_id.keys()]

# shim values not in es
for m in mutations:
    m['position_nt'] = 0
    m['ref_nt'] = 'N'
    m['alt_nt'] = 'N'
    m['metadata_id'] = sra_to_id[m.pop('sra')]

mutations_field_conversion = {
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

# use 'STAR' in DB instead of '*' to keep sqlalchemy appeased
for m in mutations_translated:
    if m['ref_aa'] == '*':
        m['ref_aa'] = 'STAR'

    if m['alt_aa'] == '*':
        m['alt_aa'] = 'STAR'

with Session(engine) as session:
    res = session.scalars(
        insert(Mutation).returning(Mutation.id, sort_by_parameter_order=True),
        mutations_translated
    )
    session.commit()
    mutation_ids_ordered = res.all()
print(mutation_ids_ordered)
print('mutations done')