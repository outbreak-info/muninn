from sqlalchemy.exc import IntegrityError

from DB.engine import get_async_session
from DB.models import Translation
import polars as pl

from utils.gathering_task_group import GatheringTaskGroup


async def insert_translation(t: Translation):
    async with get_async_session() as session:
        # todo: is there a cleaner way to get this behavior?
        try:
            session.add(t)
            await session.commit()
        except IntegrityError:
            pass


async def batch_insert_translations(translations: pl.DataFrame) -> None:
    async with GatheringTaskGroup() as tg:
        for row in translations.iter_rows(named=True):
            tg.create_task(
                insert_translation(
                    Translation(
                        allele_id = row['allele_id'],
                        amino_acid_substitution_id = row['amino_acid_substitution_id']
                    )
                )
            )
