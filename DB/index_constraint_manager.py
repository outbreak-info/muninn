from typing import Type

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql.ddl import DropConstraint, AddConstraint, DropIndex, CreateIndex
from sqlalchemy.sql.schema import Index, ColumnCollectionConstraint

from DB.engine import async_write_engine


class IndexAndConstraintManager:
    def __init__(self, base_model: Type[DeclarativeBase]):
        self._base_model = base_model
        self.tables = [sc.__table__ for sc in base_model.__subclasses__()]

    @property
    def indexes(self) -> dict[str, Index]:
        indexes_by_name = dict()
        for table in self.tables:
            for index in table.indexes:
                indexes_by_name[index.name] = index
        return indexes_by_name

    @property
    def constraints(self) -> dict[str, ColumnCollectionConstraint]:
        constraints_by_name = dict()
        for table in self.tables:
            for cnst in table.constraints:
                constraints_by_name[cnst.name] = cnst
        return constraints_by_name

    async def drop_index(self, name: str):
        async with async_write_engine.begin() as conn:
            await conn.execute(DropIndex(self.indexes[name]))

    async def restore_index(self, name: str):
        async with async_write_engine.begin() as conn:
            await conn.execute(CreateIndex(self.indexes[name]))

    async def drop_constraint(self, name: str):
        async with async_write_engine.begin() as conn:
            await conn.execute(DropConstraint(self.constraints[name]))

    async def restore_constraint(self, name: str):
        async with async_write_engine.begin() as conn:
            await conn.execute(AddConstraint(self.constraints[name]))

    async def drop_names(self, names: list[str]):
        for name in names:
            if name in self.indexes.keys():
                await self.drop_index(name)
            elif name in self.constraints.keys():
                await self.drop_constraint(name)
            else:
                raise ValueError(f'{name} not recognized as index or constraint.')

    async def restore_names(self, names: list[str]):
        for name in names:
            if name in self.indexes.keys():
                await self.restore_index(name)
            elif name in self.constraints.keys():
                await self.restore_constraint(name)
            else:
                raise ValueError(f'{name} not recognized as index or constraint.')
