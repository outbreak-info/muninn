from sqlalchemy import select, insert
from sqlalchemy.orm import Session
import json

from DB.models import Metadata, Allele, Nucleotide, AminoAcid, FluRegion
from engine import  create_pg_engine

engine = create_pg_engine()

with Session(engine) as session:
    q = select(Metadata)
    one = session.execute(q).first()[0]

    print(one.mutations)


moo = {'metadata_id' : 1, 'position_nt':0, 'ref_nt':Nucleotide.A, 'alt_nt':Nucleotide.C, 'position_aa' : 0, 'ref_aa':AminoAcid.D, 'alt_aa' : AminoAcid.E, 'region':FluRegion.HA}
noo = {'metadata_id' : 1, 'position_nt':2, 'ref_nt':Nucleotide.A, 'alt_nt':Nucleotide.C, 'position_aa' : 0, 'ref_aa':AminoAcid.D, 'alt_aa' : AminoAcid.E, 'region':FluRegion.HA}

data = [moo, noo]

with Session(engine) as session:
    res = session.scalars(
        insert(Allele).returning(Allele.id, sort_by_parameter_order=True),
        data
    )
    session.commit()
    ids = res.all()
    print(ids)