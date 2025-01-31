import csv
import os.path
import re
from glob import glob
from typing import List, Dict

from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import Metadata, FluRegion, AminoAcid, Mutation


def insert_metadata(data: List['Metadata']):
    with Session(engine) as session:
        for md in data:
            session.add(md)

        session.flush()
        sra_to_md = {md.run: md for md in data}
        session.commit()
    return sra_to_md


def get_amino(s):
    if s in {'*', '-'}:
        return AminoAcid.STAR
    else:
        return AminoAcid(s)


def read_mutations_csvs(files: List[str], sra_to_md: Dict):
    region_re = re.compile('^mutations_(\\w+)\\.csv$')
    mut_code_re = re.compile('^(.)(\\d+)(.)$')

    mutations_intermediates = []
    for file in files:
        with open(file, 'r') as f:
            filename = os.path.basename(f.name)
            region = FluRegion(region_re.match(filename).group(1))

            csvreader = csv.DictReader(f)
            csvreader.fieldnames[0] = 'mutation_code'

            for row in csvreader:
                mut_match = mut_code_re.match(row['mutation_code'])
                ref_aa = get_amino(mut_match.group(1))
                position_aa = int(mut_match.group(2))
                alt_aa = get_amino(mut_match.group(3))

                sras_present = set()
                for k, v in row.items():
                    if k.startswith('Consensus') and bool(int(v)):
                        sras_present.add(k.split('_')[1])

                mutations_intermediates.append(
                    Mutation(
                        region=region, position_aa=position_aa, ref_aa=ref_aa, alt_aa=alt_aa, position_nt=None,
                        ref_nt=None, alt_nt=None, linked_metadatas=[sra_to_md[sra] for sra in sras_present]
                    )
                )

    return mutations_intermediates


def insert_mutations(data: List['Mutation']):
    with Session(engine) as session:
        for mut in data:
            session.add(mut)
        session.commit()


def main():
    basedir = '/home/james/Documents/andersen_lab/mutations'

    unique_srr_file = f'{basedir}/unique_srr.txt'
    with open(unique_srr_file, 'r') as f:
        uniq_srrs = [l.strip() for l in f.readlines()]

    metadata = [Metadata(run=srr) for srr in uniq_srrs]
    sra_to_md = insert_metadata(metadata)

    mutations_files = glob(f'{basedir}/*.csv')
    mutations = read_mutations_csvs(mutations_files, sra_to_md)
    insert_mutations(mutations)


if __name__ == '__main__':
    main()
