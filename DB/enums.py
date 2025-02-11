from enum import Enum, unique

import sqlalchemy as sa
from sqlalchemy import CheckConstraint


class IntEnum(sa.TypeDecorator):
    impl = sa.BigInteger
    cache_ok = True

    def __init__(self, enumtype, *args, **kwargs):
        super(IntEnum, self).__init__(*args, **kwargs)
        self._enumtype = enumtype

    def process_bind_param(self, value, dialect):
        if isinstance(value, int):
            return value
        if value is None:
            return None
        return value.value

    def process_result_value(self, value, dialect):
        return self._enumtype(value)


class ConstrainableEnum(Enum):

    @classmethod
    def get_check_constraint(cls, column_name: str) -> 'CheckConstraint':
        """
        Return a check constraint that will limit to ints that map to values within the enum
        :param column_name: name of the column to be constrained
        :return: Check constraint keeping values within the valid range of the enum
        """

        # For now, we'll mandate that the valid values cover ever integer in the closed interval between the min and max
        # allowed values. This makes it simpler to write the constraint.
        values = {e.value for e in cls}
        # we already know it contains min and max, no need to check again
        contains = {i in values for i in range(min(values) + 1, max(values))}
        if False in contains:
            raise ValueError('Valid enum values have gaps within their range!')

        return CheckConstraint(
            f'{column_name} between {min(values)} and {max(values)}',
            name=f'{column_name}_in_range_of_enum_{cls.__name__}'
        )


@unique
class ConsentLevel(ConstrainableEnum):
    public = 0
    other = 1


@unique
class Nucleotide(ConstrainableEnum):
    A = 0
    C = 1
    G = 2
    T = 3
    U = 4
    R = 5
    Y = 6
    S = 7
    W = 8
    K = 9
    M = 10
    B = 11
    D = 12
    H = 13
    V = 14
    N = 15
    GAP = 16


@unique
class AminoAcid(ConstrainableEnum):
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


@unique
class FluRegion(ConstrainableEnum):
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


@unique
class Codon(ConstrainableEnum):
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
