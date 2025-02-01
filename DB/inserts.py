import csv
import os.path
import re
from glob import glob
from typing import List, Dict

from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import Metadata, FluRegion, AminoAcid, Mutation, Nucleotide, Variant, Codon


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
    elif s is None or s == '':
        return None
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
                        region=region,
                        position_aa=position_aa,
                        ref_aa=ref_aa,
                        alt_aa=alt_aa,
                        position_nt=None,
                        ref_nt=None,
                        alt_nt=None,
                        linked_metadatas=[sra_to_md[sra] for sra in sras_present]
                    )
                )

    return mutations_intermediates


def insert_mutations(data: List['Mutation']):
    with Session(engine) as session:
        for mut in data:
            session.add(mut)
        session.commit()


def get_codon(s):
    if s is None or s == '':
        return None
    return Codon(s)


def read_variants_files(files: List[str], sra_to_md: Dict):
    variants = []
    for filename in files:
        with open(filename, 'r') as f:
            csvreader = csv.DictReader(f, delimiter='\t')
            for row in csvreader:

                region_full_text = row['REGION']
                region = FluRegion(region_full_text.split('|')[0])
                alt_nt_indel = row['ALT']
                alt_nt = None
                try:
                    alt_nt = Nucleotide(alt_nt_indel)
                except ValueError:
                    pass
                ref_nt = Nucleotide(row['REF'])

                ref_codon = get_codon(row['REF_CODON'])
                alt_codon = get_codon(row['ALT_CODON'])
                ref_aa = get_amino(row['REF_AA'])
                alt_aa = get_amino(row['ALT_AA'])

                pass_ = (row['PASS'].lower() == 'true')

                position_aa = None
                try:
                    position_aa = float(row['POS_AA'])
                except:
                    pass

                variants.append(
                    Variant(
                        region=region,
                        region_full_text=region_full_text,
                        position_nt=int(row['POS']),
                        ref_nt=ref_nt,
                        alt_nt=alt_nt,
                        alt_nt_indel=alt_nt_indel,
                        ref_codon=ref_codon,
                        alt_codon=alt_codon,
                        position_aa=position_aa,
                        ref_aa=ref_aa,
                        alt_aa=alt_aa,
                        gff_feature=row['GFF_FEATURE'],
                        ref_dp=int(row['REF_DP']),
                        alt_dp=int(row['ALT_DP']),
                        ref_qual=float(row['REF_QUAL']),
                        alt_qual=float(row['ALT_QUAL']),
                        ref_rv=int(row['REF_RV']),
                        alt_rv=int(row['ALT_RV']),
                        alt_freq=float(row['ALT_FREQ']),
                        pass_=pass_,
                        pval=float(row['PVAL']),
                        total_dp=int(row['TOTAL_DP']),
                        linked_metadata=sra_to_md[row['sra']]
                    )
                )
    return variants


def insert_variants(data: List['Variant']):
    with Session(engine) as session:
        for var in data:
            session.add(var)
        session.commit()


def main():
    basedir = '/home/james/Documents/andersen_lab'

    unique_srr_file = f'{basedir}/mutations/unique_srr.txt'
    with open(unique_srr_file, 'r') as f:
        uniq_srrs = [l.strip() for l in f.readlines()]

    metadata = [Metadata(run=srr) for srr in uniq_srrs]
    sra_to_md = insert_metadata(metadata)

    mutations_files = glob(f'{basedir}/mutations/*.csv')
    mutations = read_mutations_csvs(mutations_files, sra_to_md)
    insert_mutations(mutations)

    # I didn't realize there was a combined version and I'm not rewriting the reader
    variants_files = [f'{basedir}/intrahost_dms/combined_variants.tsv']

    variants = read_variants_files(variants_files, sra_to_md)
    insert_variants(variants)

if __name__ == '__main__':
    main()
