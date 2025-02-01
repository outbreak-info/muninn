from enum import Enum, StrEnum
from typing import List
import sqlalchemy as sa
from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase


class ConsentLevel(Enum):
    public = 'public'
    other = 'other'


class Nucleotide(Enum):
    A = 'A'
    C = 'C'
    G = 'G'
    T = 'T'
    U = 'U'
    N = 'N'


class AminoAcid(Enum):
    A = 'A'
    C = 'C'
    D = 'D'
    E = 'E'
    F = 'F'
    G = 'G'
    H = 'H'
    I = 'I'
    K = 'K'
    L = 'L'
    M = 'M'
    N = 'N'
    P = 'P'
    Q = 'Q'
    R = 'R'
    S = 'S'
    T = 'T'
    V = 'V'
    W = 'W'
    Y = 'Y'
    STAR = '*'



class FluRegion(Enum):
    HA = 'HA'
    NA = 'NA'
    NP = 'NP'
    MP = 'MP'
    PB1 = 'PB1'
    PB2 = 'PB2'
    NS = 'NS'
    PB1_F2 = 'PB1-F2'
    PA = 'PA'
    PA_X = 'PA-X'
    M1 = 'M1'
    M2 = 'M2'
    NS1 = 'NS1'
    NS2 = 'NS2'


class Codon(Enum):
    AAA = 'AAA'
    AAC = 'AAC'
    AAG = 'AAG'
    AAT = 'AAT'
    ACA = 'ACA'
    ACC = 'ACC'
    ACG = 'ACG'
    ACT = 'ACT'
    AGA = 'AGA'
    AGC = 'AGC'
    AGG = 'AGG'
    AGT = 'AGT'
    ATA = 'ATA'
    ATC = 'ATC'
    ATG = 'ATG'
    ATT = 'ATT'
    CAA = 'CAA'
    CAC = 'CAC'
    CAG = 'CAG'
    CAT = 'CAT'
    CCA = 'CCA'
    CCC = 'CCC'
    CCG = 'CCG'
    CCT = 'CCT'
    CGA = 'CGA'
    CGC = 'CGC'
    CGG = 'CGG'
    CGT = 'CGT'
    CTA = 'CTA'
    CTC = 'CTC'
    CTG = 'CTG'
    CTT = 'CTT'
    GAA = 'GAA'
    GAC = 'GAC'
    GAG = 'GAG'
    GAT = 'GAT'
    GCA = 'GCA'
    GCC = 'GCC'
    GCG = 'GCG'
    GCT = 'GCT'
    GGA = 'GGA'
    GGC = 'GGC'
    GGG = 'GGG'
    GGT = 'GGT'
    GTA = 'GTA'
    GTC = 'GTC'
    GTG = 'GTG'
    GTT = 'GTT'
    TAA = 'TAA'
    TAC = 'TAC'
    TAG = 'TAG'
    TAT = 'TAT'
    TCA = 'TCA'
    TCC = 'TCC'
    TCG = 'TCG'
    TCT = 'TCT'
    TGA = 'TGA'
    TGC = 'TGC'
    TGG = 'TGG'
    TGT = 'TGT'
    TTA = 'TTA'
    TTC = 'TTC'
    TTG = 'TTG'
    TTT = 'TTT'


class BaseModel(DeclarativeBase):
    pass


metadata_mutations_table = Table(
    'metadata_mutations',
    BaseModel.metadata,
    Column('metadata_id', ForeignKey('metadata.id'), primary_key=True),
    Column('mutation_id', ForeignKey('mutations.id'), primary_key=True)
)


class Metadata(BaseModel):
    __tablename__ = 'metadata'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    run = sa.Column(sa.String, nullable=False)
    linked_mutations: Mapped[List['Mutation']] = relationship(secondary= metadata_mutations_table, back_populates='linked_metadatas')
    linked_variants: Mapped[List['Variant']] = relationship()

class Mutation(BaseModel):
    __tablename__ = 'mutations'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    position_nt = sa.Column(sa.BigInteger)
    ref_nt = sa.Column(sa.Enum(Nucleotide))
    alt_nt = sa.Column(sa.Enum(Nucleotide))
    position_aa = sa.Column(sa.Integer, nullable=False)
    ref_aa = sa.Column(sa.Enum(AminoAcid, name='amino_acid'), nullable=False)
    alt_aa = sa.Column(sa.Enum(AminoAcid, name='amino_acid'), nullable=False)
    region = sa.Column(sa.Enum(FluRegion, name='flu_region'), nullable=False)
    linked_metadatas: Mapped[List['Metadata']] = relationship(secondary=metadata_mutations_table, back_populates='linked_mutations')






class Variant(BaseModel):
    __tablename__ = 'variants'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    metadata_id: Mapped[int] = mapped_column(sa.ForeignKey('metadata.id'), nullable=False)
    linked_metadata: Mapped['Metadata'] = relationship(back_populates='linked_variants')

    position_nt = sa.Column(sa.BigInteger, nullable=False)
    ref_nt = sa.Column(sa.Enum(Nucleotide), nullable=False)
    alt_nt = sa.Column(sa.Enum(Nucleotide))
    alt_nt_indel = sa.Column(sa.String)

    ref_codon = sa.Column(sa.Enum(Codon))
    alt_codon = sa.Column(sa.Enum(Codon))

    position_aa = sa.Column(sa.Float)
    ref_aa = sa.Column(sa.Enum(AminoAcid, name='amino_acid'))
    alt_aa = sa.Column(sa.Enum(AminoAcid, name='amino_acid'))

    gff_feature = sa.Column(sa.String)
    region = sa.Column(sa.Enum(FluRegion, name='flu_region'), nullable=False)
    region_full_text = sa.Column(sa.String, nullable=False)

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


# class DmsResult(BaseModel):
#     __tablename__ = 'dms_results'
#
#     id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
#     antibody_set = sa.Column(sa.String, nullable=False)
#     entry_in_293_t_cells = sa.Column(sa.Float, nullable=False)
#     ferret_sera_escape = sa.Column(sa.Float, nullable=False)
#     ha1_ha2_h5_site = sa.Column(sa.String, nullable=False)
#     mature_h5_site = sa.Column(sa.Float, nullable=False)
#     mouse_sera_escape = sa.Column(sa.Float, nullable=False)
#     nt_changes_to_codon = sa.Column(sa.Float, nullable=False)
#     reference_h1_site = sa.Column(sa.BigInteger, nullable=False)
#     region = sa.Column(sa.Enum(FluRegion, name='flu_region'), nullable=False)
#     region_other = sa.Column(sa.String, nullable=False)
#     sa26_usage_increase = sa.Column(sa.Float, nullable=False)
#     sequential_site = sa.Column(sa.Float, nullable=False)
#     species_sera_escape = sa.Column(sa.Float, nullable=False)
#     stability = sa.Column(sa.Float, nullable=False)
#
#
# class Demixed(BaseModel):
#     __tablename__ = 'demixed'
#
#     id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
#     metadata_id = sa.Column(sa.BigInteger, sa.ForeignKey('metadata.id'), nullable=False)
#     coverage = sa.Column(sa.Float, nullable=False)
#     filename = sa.Column(sa.String, nullable=False)
#     resid = sa.Column(sa.Float, nullable=False)
#     summarized_score = sa.Column(sa.Float, nullable=False)
#     variants_filename = sa.Column(sa.String, nullable=False)
#
#
# class DemixedLineage(BaseModel):
#     __tablename__ = 'demixed_lineages'
#
#     demixed_id = sa.Column(sa.BigInteger, sa.ForeignKey('demixed.id'), primary_key=True, nullable=False)
#     lineage_id = sa.Column(sa.BigInteger, sa.ForeignKey('lineages.id'), primary_key=True, nullable=False)
#     demixed_position = sa.Column(sa.Integer, nullable=False)
#     abundance = sa.Column(sa.Float, nullable=False)
