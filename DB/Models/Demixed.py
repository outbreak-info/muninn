import sqlalchemy as sa
import BaseModel


class Demixed(BaseModel):
    __tablename__ = 'demixed'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    metadata_id = sa.Column(sa.BigInteger, sa.ForeignKey('metadata.id'), nullable=False)
    coverage = sa.Column(sa.Float, nullable=False)
    filename = sa.Column(sa.String, nullable=False)
    resid = sa.Column(sa.Float, nullable=False)
    summarized_score = sa.Column(sa.Float, nullable=False)
    variants_filename = sa.Column(sa.String, nullable=False)
