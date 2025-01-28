import sqlalchemy as sa
import BaseModel
from Enums import FluRegion


class DmsResults(BaseModel):
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
    region = sa.Column(sa.Enum(FluRegion), nullable=False)
    region_other = sa.Column(sa.String, nullable=False)
    sa26_usage_increase = sa.Column(sa.Float, nullable=False)
    sequential_site = sa.Column(sa.Float, nullable=False)
    species_sera_escape = sa.Column(sa.Float, nullable=False)
    stability = sa.Column(sa.Float, nullable=False)
