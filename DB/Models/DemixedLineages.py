import sqlalchemy as sa
import BaseModel


class DemixedLineages(BaseModel):
    __tablename__ = 'demixed_lineages'

    demixed_id = sa.Column(sa.BigInteger, sa.ForeignKey('demixed.id'), primary_key=True, nullable=False)
    lineage_id = sa.Column(sa.BigInteger, sa.ForeignKey('lineages.id'), primary_key=True, nullable=False)
    demixed_position = sa.Column(sa.Integer, nullable=False)
    abundance = sa.Column(sa.Float, nullable=False)
