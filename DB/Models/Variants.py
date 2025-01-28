import sqlalchemy as sa
import BaseModel
from Enums import Nucleotide, AminoAcid, Codon


class Variants(BaseModel):
    __tablename__ = 'variants'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    metadata_id = sa.Column(sa.BigInteger, sa.ForeignKey('metadata.id'), nullable=False)
    position_nt = sa.Column(sa.BigInteger, nullable=False)
    ref_nt = sa.Column(sa.Enum(Nucleotide), nullable=False)
    alt_nt = sa.Column(sa.Enum(Nucleotide), nullable=False)
    ref_codon = sa.Column(sa.Enum(Codon), nullable=False)
    alt_codon = sa.Column(sa.Enum(Codon), nullable=False)
    position_aa = sa.Column(sa.Float, nullable=False)
    ref_aa = sa.Column(sa.Enum(AminoAcid), nullable=False)
    alt_aa = sa.Column(sa.Enum(AminoAcid), nullable=False)
    ref_dp = sa.Column(sa.Integer, nullable=False)
    alt_dp = sa.Column(sa.Integer, nullable=False)
    ref_qual = sa.Column(sa.Float, nullable=False)
    alt_qual = sa.Column(sa.Float, nullable=False)
    ref_rv = sa.Column(sa.Integer, nullable=False)
    alt_rv = sa.Column(sa.Integer, nullable=False)
    alt_freq = sa.Column(sa.Float, nullable=False)
    pass_ = sa.Column(sa.Boolean, nullable=False)
    pval = sa.Column(sa.Float, nullable=False)
    total_dp = sa.Column(sa.Integer, nullable=False)
    dms_result_id = sa.Column(sa.BigInteger, sa.ForeignKey('dms_results.id'))
