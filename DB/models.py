from enum import Enum
from typing import List

import sqlalchemy as sa
from sqlalchemy import UniqueConstraint, CheckConstraint, MetaData
from sqlalchemy.ext.compiler import compiles
# from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase

# This is magic and I don't understand it at all.
# From https://stackoverflow.com/a/77475375
UniqueConstraint.argument_for("postgresql", 'nulls_not_distinct', None)


@compiles(UniqueConstraint, "postgresql")
def compile_create_uc(create, compiler, **kw):
    """Add NULLS NOT DISTINCT if its in args."""
    stmt = compiler.visit_unique_constraint(create, **kw)
    postgresql_opts = create.dialect_options["postgresql"]

    if postgresql_opts.get("nulls_not_distinct"):
        return stmt.rstrip().replace("UNIQUE (", "UNIQUE NULLS NOT DISTINCT (")
    return stmt


# todo: a way to turn enums into check constraints
class ConsentLevel(Enum):
    public = 0
    other = 1


class Nucleotide(Enum):
    A = 0
    C = 1
    G = 2
    T = 3
    U = 4
    N = 5


class AminoAcid(Enum):
    A = 0
    C = 1
    D = 2
    E = 3
    F = 4
    G = 5
    H = 6
    I = 7
    K = 8
    L = 9
    M = 10
    N = 11
    P = 12
    Q = 13
    R = 14
    S = 15
    T = 16
    V = 17
    W = 18
    Y = 19
    STOP = 20


class FluRegion(Enum):
    HA = 0
    NA = 1
    NP = 2
    MP = 3
    PB1 = 4
    PB2 = 5
    NS = 6
    PB1_F2 = 7
    PA = 8
    PA_X = 9
    M1 = 10
    M2 = 11
    NS1 = 12
    NS2 = 13


class Codon(Enum):
    AAA = 0
    AAC = 1
    AAG = 2
    AAT = 3
    ACA = 4
    ACC = 5
    ACG = 6
    ACT = 7
    AGA = 8
    AGC = 9
    AGG = 10
    AGT = 11
    ATA = 12
    ATC = 13
    ATG = 14
    ATT = 15
    CAA = 16
    CAC = 17
    CAG = 18
    CAT = 19
    CCA = 20
    CCC = 21
    CCG = 22
    CCT = 23
    CGA = 24
    CGC = 25
    CGG = 26
    CGT = 27
    CTA = 28
    CTC = 29
    CTG = 30
    CTT = 31
    GAA = 32
    GAC = 33
    GAG = 34
    GAT = 35
    GCA = 36
    GCC = 37
    GCG = 38
    GCT = 39
    GGA = 40
    GGC = 41
    GGG = 42
    GGT = 43
    GTA = 44
    GTC = 45
    GTG = 46
    GTT = 47
    TAA = 48
    TAC = 49
    TAG = 50
    TAT = 51
    TCA = 52
    TCC = 53
    TCG = 54
    TCT = 55
    TGA = 56
    TGC = 57
    TGG = 58
    TGT = 59
    TTA = 60
    TTC = 61
    TTG = 62
    TTT = 63


class BaseModel(DeclarativeBase):
    metadata = MetaData(
        # This will automatically name constraints, but it's still best to name them manually
        # It's possible to get conflicting names from this convention
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            # you always have to name check constraints, they just get a prefix
            "ck": "ck_%(table_name)s_`%(constraint_name)s`",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s"
        }
    )


class Sample(BaseModel):
    __tablename__ = 'samples'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    accession: Mapped[str] = mapped_column(sa.Text, nullable=False)

    related_intra_host_variants: Mapped[List['IntraHostVariant']] = relationship(back_populates='related_sample')
    related_alleles: Mapped[List['Mutation']] = relationship(back_populates='related_sample')


class Allele(BaseModel):
    __tablename__ = 'alleles'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    position_aa: Mapped[int] = mapped_column(sa.BigInteger)
    ref_aa: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    alt_aa: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    region: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)

    gff_feature: Mapped[str] = mapped_column(sa.Text, nullable=False)
    position_nt: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    alt_nt: Mapped[int] = mapped_column(sa.BigInteger)
    alt_nt_indel: Mapped[str] = mapped_column(sa.Text)

    __table_args__ = tuple(
        [
            UniqueConstraint('region', 'position_aa', 'ref_aa', 'alt_aa', name='uq_alleles_aa_values'),
            UniqueConstraint(
                'gff_feature',
                'region',
                'position_nt',
                'alt_nt',
                'alt_nt_indel',
                postgresql_nulls_not_distinct=True,
                name='uq_alleles_nt_values'
            ),
            CheckConstraint(
                'num_nulls(alt_nt, alt_nt_indel) = 1',
                name='must_have_nt_alt_xor_indel',
            ),
            CheckConstraint("gff_feature <> ''", name='gff_feature_not_empty'),
            CheckConstraint("alt_nt_indel <> ''", name='alt_nt_indel_not_empty')
        ]
    )

    related_samples: Mapped[List['Mutation']] = relationship(back_populates='related_allele')


class Mutation(BaseModel):
    __tablename__ = 'mutations'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    sample_id: Mapped[int] = mapped_column(sa.ForeignKey('samples.id'), nullable=False)
    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('sample_id', 'allele_id', name='uq_mutations_sample_allele_pair')
        ]
    )

    related_sample: Mapped['Sample'] = relationship(back_populates='related_alleles')
    related_allele: Mapped['Allele'] = relationship(back_populates='related_samples')


class IntraHostVariant(BaseModel):
    __tablename__ = 'intra_host_variants'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    sample_id: Mapped[int] = mapped_column(sa.ForeignKey('samples.id'), nullable=False)
    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)

    ref_dp: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    alt_dp: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    alt_freq: Mapped[float] = mapped_column(sa.Double, nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('sample_id', 'allele_id', name='uq_intra_host_variants_sample_allele_pair')
        ]
    )

    related_sample: Mapped['Sample'] = relationship(back_populates='related_intra_host_variants')


class DmsResult(BaseModel):
    __tablename__ = 'dms_results'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    ferret_sera_escape: Mapped[float] = mapped_column(sa.Double, nullable=False)
    stability: Mapped[float] = sa.Column(sa.Double, nullable=False)

    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('allele_id')
        ]
    )
