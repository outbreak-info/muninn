import sqlalchemy as sa
import BaseModel
from Enums import Nucleotide, AminoAcid, FluRegion


class Mutations(BaseModel):
    __tablename__ = 'mutations'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    metadata_id = sa.Column(sa.BigInteger, sa.ForeignKey('metadata.id'), nullable=False)
    position_nt = sa.Column(sa.BigInteger, nullable=False)
    ref_nt = sa.Column(sa.Enum(Nucleotide), nullable=False)
    alt_nt = sa.Column(sa.Enum(Nucleotide), nullable=False)
    position_aa = sa.Column(sa.Integer, nullable=False)
    ref_aa = sa.Column(sa.Enum(AminoAcid), nullable=False)
    alt_aa = sa.Column(sa.Enum(AminoAcid), nullable=False)
    region = sa.Column(sa.Enum(FluRegion), nullable=False)
