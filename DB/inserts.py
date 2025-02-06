import csv
from typing import List, Type

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import Sample, FluRegion, AminoAcid, Mutation, Nucleotide, IntraHostVariant, Codon, BaseModel


def insert_samples(data: List['Sample']):
    with Session(engine) as session:
        for sample in data:
            session.add(sample)

        session.flush()
        sra_to_sample = {s.accession: s for s in data}
        session.commit()
    return sra_to_sample


def get_amino(s):
    if s in {'*', '-'}:
        return AminoAcid.STAR
    elif s is None or s == '':
        return None
    else:
        return AminoAcid(s)


def get_codon(s):
    if s is None or s == '':
        return None
    return Codon(s)


def parse_and_insert_variants(files: List[str]):
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
                    alt_nt_indel = None
                except ValueError:
                    pass

                position_nt = int(row['POS'])
                gff_feature = row['GFF_FEATURE']

                if gff_feature == '':
                    gff_feature = None

                query = select(Mutation).where(
                    and_(
                        Mutation.region == region,
                        Mutation.position_nt == position_nt,
                        Mutation.alt_nt == alt_nt,
                        Mutation.alt_nt_indel == alt_nt_indel,
                        Mutation.gff_feature == gff_feature
                    )
                )

                with(Session(engine)) as session:
                    variant = IntraHostVariant(
                        ref_dp=int(row['REF_DP']),
                        alt_dp=int(row['ALT_DP']),
                        alt_freq=float(row['ALT_FREQ'])
                    )

                    mutation = session.execute(query).scalar()
                    if mutation is None:
                        mutation = Mutation(
                            position_nt=position_nt,
                            alt_nt=alt_nt,
                            alt_nt_indel=alt_nt_indel,
                            region=region,
                            gff_feature=gff_feature
                        )

                        session.add(mutation)
                        session.commit()

                    variant.mutation = mutation

                    sample = session.execute(
                        select(Sample).where(Sample.accession == row['sra'])
                    ).scalar()
                    sample.related_mutations.append(variant)
                    session.add(variant)
                    session.commit()


def table_has_rows(table: Type['BaseModel']) -> bool:
    with Session(engine) as session:
        return session.query(table).count() > 0


def main():
    basedir = '/home/james/Documents/andersen_lab'

    unique_srr_file = f'{basedir}/mutations/unique_srr.txt'
    with open(unique_srr_file, 'r') as f:
        uniq_srrs = [l.strip() for l in f.readlines()]

    samples = [Sample(accession=srr) for srr in uniq_srrs]

    if not table_has_rows(Sample):
        sra_to_sample = insert_samples(samples)
    else:
        print('Samples already has data, skipping...')

    # mutations_files = glob(f'{basedir}/mutations/*.csv')
    # mutations = read_mutations_csvs(mutations_files, sra_to_sample)
    # insert_mutations(mutations)

    # I didn't realize there was a combined version and I'm not rewriting the reader
    variants_files = [f'{basedir}/intrahost_dms/combined_variants.tsv']

    if not table_has_rows(IntraHostVariant):
        parse_and_insert_variants(variants_files)
    else:
        print("IntraHostVariants already has data, skipping...")


if __name__ == '__main__':
    main()
