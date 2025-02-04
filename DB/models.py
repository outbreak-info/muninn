from enum import Enum
from typing import List

import sqlalchemy as sa
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


class Sample(BaseModel):
    __tablename__ = 'samples'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    accession = sa.Column(sa.String, nullable=False)

    related_mutations: Mapped[List['IntraHostVariant']] = relationship(back_populates='sample')


class Mutation(BaseModel):
    __tablename__ = 'mutations'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    position_aa = sa.Column(sa.Integer)
    ref_aa = sa.Column(sa.Enum(AminoAcid, name='amino_acid'))
    alt_aa = sa.Column(sa.Enum(AminoAcid, name='amino_acid'))
    region = sa.Column(sa.Enum(FluRegion, name='flu_region'), nullable=False)

    gff_feature = sa.Column(sa.String, nullable=False)
    position_nt = sa.Column(sa.Integer, nullable=False)
    alt_nt = sa.Column(sa.Enum(Nucleotide, name='nucleotide'))
    alt_nt_indel = sa.Column(sa.String)

    related_samples: Mapped[List['IntraHostVariant']] = relationship(back_populates='mutation')


class IntraHostVariant(BaseModel):
    __tablename__ = 'intra_host_variants'

    sample_id: Mapped[int] = mapped_column(sa.ForeignKey('samples.id'), primary_key=True)
    mutation_id: Mapped[int] = mapped_column(sa.ForeignKey('mutations.id'), primary_key=True)

    ref_dp = sa.Column(sa.Integer, nullable=False)
    alt_dp = sa.Column(sa.Integer, nullable=False)
    alt_freq = sa.Column(sa.Float, nullable=False)

    sample: Mapped['Sample'] = relationship(back_populates='related_mutations')
    mutation: Mapped['Mutation'] = relationship(back_populates='related_samples')


class DmsResult(BaseModel):
    __tablename__ = 'dms_results'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    ferret_sera_escape = sa.Column(sa.Float, nullable=False)
    stability = sa.Column(sa.Float, nullable=False)

    mutation_id: Mapped[int] = mapped_column(sa.ForeignKey('mutations.id'), nullable=False)
