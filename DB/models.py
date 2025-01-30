from enum import Enum, StrEnum
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


class Metadata(BaseModel):
    __tablename__ = 'metadata'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    mutations: Mapped[List['Mutation']] = relationship(back_populates='linked_metadata')
    variants: Mapped[List['Variant']] = relationship(back_populates='linked_metadata')
    assay_type = sa.Column(sa.String, nullable=False)
    avg_spot_len = sa.Column(sa.Float, nullable=False)
    bases = sa.Column(sa.BigInteger, nullable=False)
    bio_project = sa.Column(sa.String, nullable=False)
    bio_sample = sa.Column(sa.String)
    bio_sample_accession = sa.Column(sa.String, nullable=False)
    bio_sample_model = sa.Column(sa.String, nullable=False)
    bytes = sa.Column(sa.BigInteger, nullable=False)
    center_name = sa.Column(sa.String, nullable=False)
    collection_date = sa.Column(sa.Date, nullable=False)
    consent = sa.Column(sa.Enum(ConsentLevel, name='consent_level'), nullable=False)
    create_date = sa.Column(sa.Date, nullable=False)
    datastore_filetype = sa.Column(sa.String, nullable=False)
    datastore_provider = sa.Column(sa.String, nullable=False)
    datastore_region = sa.Column(sa.String, nullable=False)
    experiment = sa.Column(sa.String, nullable=False)
    geo_loc_name = sa.Column(sa.String, nullable=False)
    geo_loc_name_country = sa.Column(sa.String, nullable=False)
    geo_loc_name_country_continent = sa.Column(sa.String)
    host = sa.Column(sa.String, nullable=False)
    instrument = sa.Column(sa.String, nullable=False)
    isolate = sa.Column(sa.String, nullable=False)
    isolation_source = sa.Column(sa.String)
    library_layout = sa.Column(sa.String, nullable=False)
    library_name = sa.Column(sa.String, nullable=False)
    library_selection = sa.Column(sa.String, nullable=False)
    library_source = sa.Column(sa.String, nullable=False)
    organism = sa.Column(sa.String, nullable=False)
    platform = sa.Column(sa.String, nullable=False)
    release_date = sa.Column(sa.Date, nullable=False)
    run = sa.Column(sa.String, nullable=False)
    sample_name = sa.Column(sa.String, nullable=False)
    serotype = sa.Column(sa.String, nullable=False)
    sra_study = sa.Column(sa.String, nullable=False)
    version = sa.Column(sa.String, nullable=False)


class Mutation(BaseModel):
    __tablename__ = 'mutations'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    metadata_id: Mapped[int] = mapped_column(sa.ForeignKey('metadata.id'), nullable=False)
    linked_metadata: Mapped['Metadata'] = relationship(back_populates='mutations')
    position_nt = sa.Column(sa.BigInteger, nullable=False)
    ref_nt = sa.Column(sa.Enum(Nucleotide), nullable=False)
    alt_nt = sa.Column(sa.Enum(Nucleotide), nullable=False)
    position_aa = sa.Column(sa.Integer, nullable=False)
    ref_aa = sa.Column(sa.Enum(AminoAcid, name='amino_acid'), nullable=False)
    alt_aa = sa.Column(sa.Enum(AminoAcid, name='amino_acid'), nullable=False)
    region = sa.Column(sa.Enum(FluRegion, name='flu_region'), nullable=False)


class Variant(BaseModel):
    __tablename__ = 'variants'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    metadata_id: Mapped[int] = mapped_column(sa.ForeignKey('metadata.id'), nullable=False)
    linked_metadata: Mapped['Metadata'] = relationship(back_populates='variants')
    position_nt = sa.Column(sa.BigInteger, nullable=False)
    ref_nt = sa.Column(sa.Enum(Nucleotide), nullable=False)
    alt_nt = sa.Column(sa.Enum(Nucleotide), nullable=False)
    ref_codon = sa.Column(sa.Enum(Codon), nullable=False)
    alt_codon = sa.Column(sa.Enum(Codon), nullable=False)
    position_aa = sa.Column(sa.Float, nullable=False)
    ref_aa = sa.Column(sa.Enum(AminoAcid, name='amino_acid'), nullable=False)
    alt_aa = sa.Column(sa.Enum(AminoAcid, name='amino_acid'), nullable=False)
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


class DmsResult(BaseModel):
    __tablename__ = 'dms_results'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    antibody_set = sa.Column(sa.String, nullable=False)
    entry_in_293_t_cells = sa.Column(sa.Float, nullable=False)
    ferret_sera_escape = sa.Column(sa.Float, nullable=False)
    ha1_ha2_h5_site = sa.Column(sa.String, nullable=False)
    mature_h5_site = sa.Column(sa.Float, nullable=False)
    mouse_sera_escape = sa.Column(sa.Float, nullable=False)
    nt_changes_to_codon = sa.Column(sa.Float, nullable=False)
    reference_h1_site = sa.Column(sa.BigInteger, nullable=False)
    region = sa.Column(sa.Enum(FluRegion, name='flu_region'), nullable=False)
    region_other = sa.Column(sa.String, nullable=False)
    sa26_usage_increase = sa.Column(sa.Float, nullable=False)
    sequential_site = sa.Column(sa.Float, nullable=False)
    species_sera_escape = sa.Column(sa.Float, nullable=False)
    stability = sa.Column(sa.Float, nullable=False)


class Demixed(BaseModel):
    __tablename__ = 'demixed'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    metadata_id = sa.Column(sa.BigInteger, sa.ForeignKey('metadata.id'), nullable=False)
    coverage = sa.Column(sa.Float, nullable=False)
    filename = sa.Column(sa.String, nullable=False)
    resid = sa.Column(sa.Float, nullable=False)
    summarized_score = sa.Column(sa.Float, nullable=False)
    variants_filename = sa.Column(sa.String, nullable=False)


class DemixedLineage(BaseModel):
    __tablename__ = 'demixed_lineages'

    demixed_id = sa.Column(sa.BigInteger, sa.ForeignKey('demixed.id'), primary_key=True, nullable=False)
    lineage_id = sa.Column(sa.BigInteger, sa.ForeignKey('lineages.id'), primary_key=True, nullable=False)
    demixed_position = sa.Column(sa.Integer, nullable=False)
    abundance = sa.Column(sa.Float, nullable=False)
