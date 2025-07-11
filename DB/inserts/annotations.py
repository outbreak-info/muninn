from typing import Set

from DB.engine import get_async_write_session
from DB.models import Annotation, AnnotationAminoAcid
from DB.queries.amino_acid_substitutions import get_aa_ids_for_annotation_effect
from utils.errors import DuplicateAnnotationError


async def insert_annotation(a: Annotation, amino_acid_ids: Set[int]) -> int:
    # make sure we aren't making a duplicate
    amino_acid_ids = set(amino_acid_ids)  # no, you're paranoid
    existing_id_sets = await get_aa_ids_for_annotation_effect(a.effect_id)
    if amino_acid_ids in existing_id_sets:
        raise DuplicateAnnotationError(message='No')

    async with get_async_write_session() as session:
        session.add(a)
        await session.commit()
        await session.refresh(a)

        amino_acids = [
            AnnotationAminoAcid(amino_acid_id=aaid, annotation_id=a.id)
            for aaid in amino_acid_ids
        ]
        session.add_all(amino_acids)
        await session.commit()
    return a.id