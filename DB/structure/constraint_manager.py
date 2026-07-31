from typing import Iterable

from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session


class ConstraintManager:
    _dropped_cons_defs = dict()

    @classmethod
    async def drop_constraints(cls, connames: Iterable[str]):
        commit_defs = dict()
        async with get_async_write_session() as session:
            for conname in connames:
                res = await session.execute(
                    text(
                        'select ut.relname, c.oid\n'
                        'from pg_constraint c\n'
                        'inner join pg_stat_user_tables ut on ut.relid = c.conrelid\n'
                        'where conname = :conname'
                    ),
                    {'conname': conname}
                )
                relname, oid = res.one()

                res = await session.execute(text('select pg_get_constraintdef(:oid);'), {'oid': oid})
                def_ = res.one()[0]

                create_statement = f'alter table {relname} add constraint {conname} {def_};'

                await session.execute(
                    text(f'alter table {relname} drop constraint {conname};'),
                )
                commit_defs[conname] = create_statement

            await session.commit()
        cls._dropped_cons_defs.update(commit_defs)

    @classmethod
    async def restore_constraints(cls, connames: Iterable[str]):
        async with get_async_write_session() as session:
            for conname in connames:
                await session.execute(text(cls._dropped_cons_defs[conname]))
            await session.commit()
        for conname in connames:
            cls._dropped_cons_defs.pop(conname)

    @classmethod
    async def drop_constraint(cls, conname: str):
        await cls.drop_constraints([conname])

    @classmethod
    async def restore_constraint(cls, conname: str):
        await cls.restore_constraints([conname])
