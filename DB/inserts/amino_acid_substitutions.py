from sqlalchemy import and_, select

from DB.engine import get_async_session
from DB.models import AminoAcidSubstitution


async def find_or_insert_aa_sub(aas: AminoAcidSubstitution) -> int:
    async with get_async_session() as session:
        id_: int = await session.scalar(
            select(AminoAcidSubstitution.id)
            .where(
                and_(
                    AminoAcidSubstitution.allele_id == aas.allele_id,
                    AminoAcidSubstitution.gff_feature == aas.gff_feature,
                    AminoAcidSubstitution.position_aa == aas.position_aa,
                    AminoAcidSubstitution.alt_aa == aas.position_aa
                )
            )
        )

        if id_ is None:
            session.add(aas)
            await session.commit()
            await session.refresh(aas)
            id_ = aas.id
    return id_
