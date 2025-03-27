from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import GeoLocation


async def find_or_insert_geo_location(gl: GeoLocation) -> int:
    async with get_async_session() as session:
        id_: int = await session.scalar(
            select(GeoLocation.id)
            .where(
                and_(
                    GeoLocation.country_name == gl.country_name,
                    GeoLocation.region_name == gl.region_name,
                    GeoLocation.locality_name == gl.locality_name
                )
            )
        )

        if id_ is None:
            session.add(gl)
            await session.commit()
            await session.refresh(gl)
            id_ = gl.id
    return id_
