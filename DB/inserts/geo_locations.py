from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session
from DB.models import GeoLocation
from utils.constants import TableNames, ColumnNames


async def find_or_insert_geo_location(gl: GeoLocation) -> int:
    async with get_async_write_session() as session:
        id_ = await session.scalar(text(
            f'select id from {TableNames.geo_locations}\n'
            f'where {ColumnNames.country_name} = :country_name\n'
            f'and {ColumnNames.admin1_name} = :admin1_name\n'
            f'and {ColumnNames.admin2_name} = :admin2_name\n'
            f'and {ColumnNames.admin3_name} = :admin3_name;'
        ),
            {
                'country_name': gl.country_name,
                'admin1_name': gl.admin1_name,
                'admin2_name': gl.admin2_name,
                'admin3_name': gl.admin3_name
            }
        )

        if id_ is None:
            session.add(gl)
            await session.commit()
            await session.refresh(gl)
            id_ = gl.id
    return id_
