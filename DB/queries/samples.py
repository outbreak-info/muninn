from typing import List

from sqlalchemy import select, text
from sqlalchemy.orm import Session, contains_eager

from DB.engine import engine
from DB.models import Sample, Mutation, GeoLocation, Allele, AminoAcidSubstitution, IntraHostVariant
from api.models import SampleInfo
from parser.parser import parser


def get_sample_by_id(sample_id: int) -> SampleInfo | None:
    query = (
        select(Sample, GeoLocation)
        .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
        .options(contains_eager(Sample.r_geo_location))
        .where(Sample.id == sample_id)
    )

    with Session(engine) as session:
        result = session.execute(query).scalar()
    if result is None:
        return None
    return SampleInfo.from_db_object(result)


def get_samples(query: str) -> List['SampleInfo']:
    # todo: bind parameters
    user_defined_query = parser.parse(query)

    samples_query = (
        select(Sample, GeoLocation)
        .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
        .options(contains_eager(Sample.r_geo_location))
        .where(text(user_defined_query))
    )

    with Session(engine) as session:
        samples = session.execute(samples_query).scalars()
        out_data = [SampleInfo.from_db_object(s) for s in samples]
    return out_data


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
    user_defined_query = parser.parse(query)

    # todo: bind parameters
    samples_query = (
        select(Sample, GeoLocation)
        .join(GeoLocation, Sample.geo_location_id == GeoLocation.id, isouter=True)
        .options(contains_eager(Sample.r_geo_location))
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
        samples = session.execute(samples_query).unique().scalars()
        out_data = []
        for s in samples:
            out_data.append(
                SampleInfo.from_db_object(s)
            )
    return out_data


# select * from samples s
# left join geo_locations gl on gl.id = s.geo_location_id
# where s.id in (
#         select ihv.sample_id from (
#                 intra_host_variants ihv
#                 left join alleles a on ihv.allele_id = a.id
#                 left join amino_acid_substitutions aas on a.id = aas.allele_id
#         ) where (
#                 region = 'HA' and ref_aa = 'M' and ref_dp = 0
#         )
# );
def get_samples_by_variant(query: str) -> List['SampleInfo']:
    # todo: bind parameters, id will fail
    user_query = parser.parse(query)

    samples_query = (
        select(Sample, GeoLocation)
        .join(GeoLocation, Sample.geo_location_id == GeoLocation.id, isouter=True)
        .options(contains_eager(Sample.r_geo_location))
        .where(
            Sample.id.in_(
                select(IntraHostVariant.sample_id)
                .join(Allele, Allele.id == IntraHostVariant.allele_id, isouter=True)
                .join(AminoAcidSubstitution, AminoAcidSubstitution.allele_id == Allele.id, isouter=True)
                .where(text(user_query))
            )
        )
    )

    with Session(engine) as session:
        samples = session.execute(samples_query).unique().scalars()
        out_data = [SampleInfo.from_db_object(s) for s in samples]
    return out_data
