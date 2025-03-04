from typing import List

from sqlalchemy import select, text
from sqlalchemy.orm import Session, joinedload

from DB.engine import engine
from DB.models import Sample, IntraHostVariant, Allele, AminoAcidSubstitution
from api.models import VariantInfo, PydAminoAcidSubstitution


def get_variants_for_sample(query: str) -> List['VariantInfo']:

    query = (
        select(IntraHostVariant, Allele, AminoAcidSubstitution)
        .join(Allele, IntraHostVariant.allele_id == Allele.id, isouter=True)
        .options(joinedload(IntraHostVariant.r_allele))
        .join(AminoAcidSubstitution, Allele.id == AminoAcidSubstitution.allele_id, isouter=True)
        .options(joinedload(Allele.r_amino_subs))
        .filter(
            IntraHostVariant.sample_id.in_(
                text(Sample.parse_query(query))
            )
        )
    )

    with (Session(engine) as session):
        results = session.execute(query).unique().scalars()
        out_data = []
        for ihv in results:
            r_amino_subs: List['PydAminoAcidSubstitution'] = [aas.to_pyd_model() for aas in ihv.r_allele.r_amino_subs]
            variant_info = VariantInfo(
                id=ihv.id,
                sample_id=ihv.sample_id,
                allele_id=ihv.allele_id,
                ref_dp=ihv.ref_dp,
                alt_dp=ihv.alt_dp,
                alt_freq=ihv.alt_freq,
                region=ihv.r_allele.region,
                position_nt=ihv.r_allele.position_nt,
                ref_nt=ihv.r_allele.ref_nt,
                alt_nt=ihv.r_allele.alt_nt,
                amino_acid_mutations=r_amino_subs
            )
            out_data.append(variant_info)
        return out_data
