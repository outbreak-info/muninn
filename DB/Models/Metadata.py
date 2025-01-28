import sqlalchemy as sa
import BaseModel
from Enums import ConsentLevel


class Metadata(BaseModel):
    __tablename__ = 'metadata'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    assay_type = sa.Column(sa.String, nullable=False)
    avg_spot_len = sa.Column(sa.Float, nullable=False)
    bases = sa.Column(sa.BigInteger, nullable=False)
    bio_project = sa.Column(sa.String, nullable=False)
    bio_sample = sa.Column(sa.String)
    bio_sample_accession = sa.Column(sa.String, nullable=False)
    bio_sample_model = sa.Column(sa.String, nullable=False)
    bytes = sa.Column(sa.BigInteger, nullable=False)
    center_name = sa.Column(sa.sa.String, nullable=False)
    collection_date = sa.Column(sa.Date, nullable=False)
    consent = sa.Column(sa.Enum(ConsentLevel), nullable=False)
    create_date = sa.Column(sa.Date, nullable=False)
    datastore_filetype = sa.Column(sa.String, nullable=False)
    datastore_provider = sa.Column(sa.String, nullable=False)
    datastore_region = sa.Column(sa.String, nullable=False)
    experiment = sa.Column(sa.String, nullable=False)
    geo_loc_name = sa.Column(sa.String, nullable=False)
    geo_loc_name_country = sa.Column(sa.String, nullable=False)
    geo_loc_name_country_continent = sa.Column(sa.String)
    host = sa.Column(sa.String, nullable=False)
    instrument = sa.Column(sa.String, nullable=False)
    isolate = sa.Column(sa.String, nullable=False)
    isolation_source = sa.Column(sa.String)
    library_layout = sa.Column(sa.String, nullable=False)
    library_name = sa.Column(sa.String, nullable=False)
    library_selection = sa.Column(sa.String, nullable=False)
    library_source = sa.Column(sa.String, nullable=False)
    organism = sa.Column(sa.String, nullable=False)
    platform = sa.Column(sa.String, nullable=False)
    release_date = sa.Column(sa.Date, nullable=False)
    run = sa.Column(sa.String, nullable=False)
    sample_name = sa.Column(sa.String, nullable=False)
    serotype = sa.Column(sa.String, nullable=False)
    sra_study = sa.Column(sa.String, nullable=False)
    version = sa.Column(sa.String, nullable=False)
