import polars as pl
from sqlalchemy import select, and_

from DB.engine import get_uri_for_polars, get_async_session
from DB.models import AminoAcidSubstitution
from utils.constants import StandardColumnNames
from utils.errors import NotFoundError


async def get_all_amino_acid_subs_as_pl_df() -> pl.DataFrame:
    return pl.read_database_uri(
        query='select * from amino_acid_substitutions;',
        uri=get_uri_for_polars()
    ).rename({'id': StandardColumnNames.amino_acid_substitution_id})


async def find_aa_sub(aas: AminoAcidSubstitution) -> int:
    if None in {aas.alt_aa, aas.ref_aa, aas.position_aa, aas.gff_feature}:
        raise RuntimeError('Required fields absent from aas')

    async with get_async_session() as session:
        id_ = await session.scalar(
            select(AminoAcidSubstitution.id)
            .where(
                and_(
                    AminoAcidSubstitution.gff_feature == aas.gff_feature,
                    AminoAcidSubstitution.position_aa == aas.position_aa,
                    AminoAcidSubstitution.alt_aa == aas.alt_aa,
                    AminoAcidSubstitution.ref_aa == aas.ref_aa
                )
            )
        )
    if id_ is None:
        raise NotFoundError('No amino sub found')
    return id_
