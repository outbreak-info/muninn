"""
Quick and dirty script to normalize host and isolation source
replace anything outside \w
"""
import asyncio
import re
from sqlalchemy import select

from DB.engine import get_async_session
from DB.models import Sample

non_word_pattern = re.compile(r'\W')
multi_space_pattern = re.compile(r' {2,}')


def norm(s: str | None) -> str | None:
    if s is None:
        return None
    out = non_word_pattern.sub(' ', s)
    out = multi_space_pattern.sub(' ', out)
    out = out.lower()
    return out


async def normalize_hosts_and_isolation_sources():
    async with get_async_session() as session:
        samples = await session.scalars(select(Sample))
        for sample in samples:
            sample.host = norm(sample.host)
            sample.isolation_source = norm(sample.isolation_source)
            await session.commit()


if __name__ == '__main__':
    asyncio.run(normalize_hosts_and_isolation_sources())
