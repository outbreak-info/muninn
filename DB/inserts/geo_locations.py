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
                    GeoLocation.admin1_name == gl.admin1_name,
                    GeoLocation.admin2_name == gl.admin2_name,
                    GeoLocation.admin3_name == gl.admin3_name
                )
            )
        )

        if id_ is None:
            session.add(gl)
            await session.commit()
            await session.refresh(gl)
            id_ = gl.id
    return id_


async def find_or_insert_geo_location_foo(
    foo: tuple
) -> int:
    country_name = admin1_name = admin2_name = admin3_name = None
    country_name = foo[0]
    if len(foo) > 1:
        admin1_name = foo[1]
    if len(foo) > 2:
        admin2_name = foo[2]
    if len(foo) > 3:
        admin1_name = foo[3]

    return await find_or_insert_geo_location(
        GeoLocation(
            country_name=country_name,
            admin1_name=admin1_name,
            admin2_name=admin2_name,
            admin3_name=admin3_name
        )
    )