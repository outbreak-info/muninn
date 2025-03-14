from typing import List

from sqlalchemy import select, and_, text
from sqlalchemy.orm import Session, joinedload

from DB.engine import engine
from DB.models import Sample, Mutation, GeoLocation, Allele, AminoAcidSubstitution
from api.models import SampleInfo
from parser.parser import parser


def get_samples_via_mutation_by_allele_id(allele_id: int) -> List['Sample']:
    mutations_query = select(Mutation).where(Mutation.allele_id == allele_id).with_only_columns(Mutation.sample_id)
    samples_query = select(Sample).filter(Sample.id.in_(mutations_query))

    with Session(engine) as session:
        samples = session.execute(samples_query).scalars()
        return [s for s in samples]


# select * from samples s
# left join geo_locations gl on gl.id = s.geo_location_id
# where s.id in (
#         select m.sample_id from mutations m where m.allele_id in (
#                 select a.id from (
#                         alleles a left join amino_acid_substitutions aas on a.id = aas.allele_id
#                 ) where  (
#                         region = 'HA' and ref_aa = 'M'
#                 )
#         )
# );


def get_samples_by_mutation(query: str) -> List['SampleInfo']:

    # todo: any query that uses 'id' as a field will fail due to ambiguity between alleles.id and aas.id
    user_defined_query = parser.parse(query)

    # todo: bind parameters
    query = (
        select(Sample, GeoLocation)
        .join(GeoLocation, Sample.geo_location_id == GeoLocation.id, isouter=True)
        .options(joinedload(Sample.r_geo_location))
        .where(
            Sample.id.in_(
                select(Mutation.sample_id)
                .where(
                    Mutation.allele_id.in_(
                        select(Allele.id)
                        .join(AminoAcidSubstitution, Allele.id == AminoAcidSubstitution.allele_id, isouter=True)
                        .where(text(user_defined_query))
                    )
                )
            )
        )
    )

    with Session(engine) as session:
        samples = session.execute(query).unique().scalars()
        out_data = []
        for s in samples:
            out_data.append(
                SampleInfo(
                    id=s.id,
                    accession=s.accession,
                    consent_level=s.consent_level,
                    bio_project=s.bio_project,
                    bio_sample=s.bio_sample,
                    bio_sample_accession=s.bio_sample_accession,
                    bio_sample_model=s.bio_sample_model,
                    center_name=s.center_name,
                    experiment=s.experiment,
                    host=s.host,
                    instrument=s.instrument,
                    platform=s.platform,
                    isolate=s.isolate,
                    library_name=s.library_name,
                    library_layout=s.library_layout,
                    library_selection=s.library_selection,
                    library_source=s.library_source,
                    organism=s.organism,
                    is_retracted=s.is_retracted,
                    retraction_detected_date=s.retraction_detected_date,
                    isolation_source=s.isolation_source,
                    release_date=s.release_date,
                    creation_date=s.creation_date,
                    version=s.version,
                    sample_name=s.sample_name,
                    sra_study=s.sra_study,
                    serotype=s.serotype,
                    assay_type=s.assay_type,
                    avg_spot_length=s.avg_spot_length,
                    bases=s.bases,
                    bytes=s.bytes,
                    datastore_filetype=s.datastore_filetype,
                    datastore_region=s.datastore_region,
                    datastore_provider=s.datastore_provider,
                    collection_start_date=s.collection_start_date,
                    collection_end_date=s.collection_end_date,
                    geo_location_id=s.geo_location_id,
                    geo_country_name=s.r_geo_location.country_name,
                    geo_region_name=s.r_geo_location.region_name,
                    geo_locality_name=s.r_geo_location.locality_name
                )
            )
    return out_data
