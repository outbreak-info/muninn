import sqlalchemy as sa
import BaseModel


class Lineages(BaseModel):
    __tablename__ = 'lineages'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)
    alias = sa.Column(sa.String, nullable=False)
