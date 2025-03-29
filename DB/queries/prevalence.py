import re
from typing import List

from sqlalchemy import select, and_, ColumnElement
from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import IntraHostVariant, Sample, Allele, AminoAcidSubstitution, Translation
from api.models import VariantFreqInfo
from utils.constants import CHANGE_PATTERN


def get_samples_variant_freq_by_aa_change(change: str) -> List[VariantFreqInfo]:
    region, ref_aa, position_aa, alt_aa = _parse_change_string(change)

    where_clause = and_(
        Allele.region == region,
        AminoAcidSubstitution.ref_aa == ref_aa,
        AminoAcidSubstitution.position_aa == position_aa,
        AminoAcidSubstitution.alt_aa == alt_aa
    )

    return _get_samples_variant_freq(where_clause)


def get_samples_variant_freq_by_nt_change(change: str) -> List[VariantFreqInfo]:
    region, ref_nt, position_nt, alt_nt = _parse_change_string(change)

    where_clause = and_(
        Allele.region == region,
        Allele.ref_nt == ref_nt,
        Allele.position_nt == position_nt,
        Allele.alt_nt == alt_nt
    )

    return _get_samples_variant_freq(where_clause)


def _get_samples_variant_freq(where_clause: ColumnElement[bool]) -> List[VariantFreqInfo]:
    query = (
        select(IntraHostVariant.alt_freq, Sample.accession, Allele.id, Translation.id, AminoAcidSubstitution.id)
        .join(Sample, Sample.id == IntraHostVariant.sample_id, isouter=True)
        .join(Allele, Allele.id == IntraHostVariant.allele_id, isouter=True)
        .join(Translation, Allele.id == Translation.allele_id, isouter=True)
        .join(AminoAcidSubstitution, Translation.amino_acid_substitution_id == AminoAcidSubstitution.id, isouter=True)
        .where(where_clause)
    )

    with Session(engine) as session:
        res = session.execute(query).all()
    out_data = []
    for r in res:
        out_data.append(
            VariantFreqInfo(
                alt_freq=r[0],
                accession=r[1],
                allele_id=r[2],
                translation_id=r[3],
                amino_sub_id=r[4]
            )
        )
    return out_data


def _parse_change_string(change: str) -> (str, str, int, str):
    pattern = re.compile(CHANGE_PATTERN)
    match = pattern.fullmatch(change)

    if match is None:
        raise ValueError(f'This change string fails validation: {change}')

    region = match[1]
    ref = match[2]
    position = int(match[3])
    alt = match[4]

    return region, ref, position, alt
