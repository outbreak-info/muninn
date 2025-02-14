import csv
import json
import sys
from glob import glob
from typing import List, Type

from sqlalchemy import select, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from DB.engine import engine
from DB.enums import AminoAcid, Codon, FluRegion, Nucleotide, ConsentLevel
from DB.models import Sample, Allele, IntraHostVariant, BaseModel, Mutation


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
        return AminoAcid.STOP
    elif s is None or s == '':
        return None
    else:
        return AminoAcid[s]


def get_codon(s):
    if s is None or s == '':
        return None
    return Codon(s)


def find_or_add_sample(session: Session, accession: str) -> Sample:
    sample = session.execute(
        select(Sample).where(Sample.accession == accession)
    ).scalar()
    if sample is None:
        sample = Sample(accession=accession, consent_level=ConsentLevel.public)
        session.add(sample)
    return sample


def parse_and_insert_variants(files: List[str]):
    for filename in files:
        with (open(filename, 'r') as f):
            csvreader = csv.DictReader(f, delimiter='\t')
            for row in csvreader:

                region_full_text = row['REGION']
                region = FluRegion[region_full_text.split('|')[0]]

                alt_nt_indel = row['ALT']
                alt_nt = None
                try:
                    alt_nt = Nucleotide[alt_nt_indel]
                    alt_nt_indel = None
                except KeyError:
                    pass

                position_nt = int(row['POS'])
                gff_feature = row['GFF_FEATURE']

                # todo: need a way to handle * and - aminos better
                ref_aa = None
                try:
                    ref_aa = get_amino(row['REF_AA'])
                except KeyError:
                    pass

                alt_aa = None
                try:
                    alt_aa = get_amino(row['ALT_AA'])
                except KeyError:
                    pass

                position_aa = None
                try:
                    position_aa = int(float(row['POS_AA']))
                except TypeError:
                    pass
                except ValueError:
                    pass

                if gff_feature == '':
                    gff_feature = None

                query = select(Allele).where(
                    and_(
                        Allele.region == region,
                        Allele.position_nt == position_nt,
                        Allele.alt_nt == alt_nt,
                        Allele.alt_nt_indel == alt_nt_indel
                    )
                )

                with(Session(engine)) as session:
                    try:
                        variant = IntraHostVariant(
                            ref_dp=int(row['REF_DP']),
                            alt_dp=int(row['ALT_DP']),
                            alt_freq=float(row['ALT_FREQ'])
                        )

                        allele = session.execute(query).scalar()
                        # todo: for now I'm going to totally ignore amino acids
                        if allele is None:
                            allele = Allele(
                                position_nt=position_nt,
                                alt_nt_indel=alt_nt_indel,
                                alt_nt=alt_nt,
                                region=region,
                                # gff_feature=gff_feature,
                                # position_aa=position_aa,
                                # ref_aa=ref_aa,
                                # alt_aa=alt_aa
                            )

                            session.add(allele)
                            session.commit()

                        # else:
                        #     # check for mismatches in AA data
                        #     if (
                        #             allele.gff_feature is not None and gff_feature is not None and allele.gff_feature != gff_feature) or (
                        #             allele.ref_aa is not None and ref_aa is not None and allele.ref_aa != ref_aa) or (
                        #             allele.alt_aa is not None and alt_aa is not None and allele.alt_aa != alt_aa) or (
                        #             allele.position_aa is not None and position_aa is not None and allele.position_aa != position_aa
                        #     ):
                        #         print(
                        #             f'AA data mismatch (existing / new): gff_feature: {allele.gff_feature} / {gff_feature}, position_aa: {allele.position_aa} / {position_aa}, ref_aa: {allele.ref_aa} / {ref_aa}, alt_aa: {allele.alt_aa} / {alt_aa}'
                        #             )
                        #         # todo: need to figure out what to do with these, for now just skip them
                        #         continue
                        #
                        #
                        #     # If the existing allele doesn't have aa data, and this one does, add it.
                        #     if not allele.has_aa_data() and None not in {gff_feature, position_aa, ref_aa, alt_aa}:
                        #         allele.gff_feature = gff_feature
                        #         allele.position_aa = position_aa
                        #         allele.ref_aa = ref_aa
                        #         allele.alt_aa = alt_aa
                        #         allele.has_aa_data()

                        variant.related_allele = allele

                        sample = find_or_add_sample(session, row['sra'])
                        sample.related_intra_host_variants.append(variant)
                        session.add(variant)
                        session.commit()

                    except IntegrityError as e:
                        print(e)


def parse_and_insert_mutations(mutations_files):
    for file in mutations_files:
        with open(file, 'r') as f:
            for row in json.load(f):
                accession = row['sra']
                region = FluRegion[row['region']]
                position_nt = int(row['pos'])
                alt_nt = Nucleotide[row['alt']]
                # todo: we're just going to totally ignore AA stuff for the moment

                with Session(engine) as session:
                    sample = find_or_add_sample(session, accession)

                    # todo: since we're ignoring aa stuff, just grab the first one that matches on nt data
                    allele = session.execute(
                        select(Allele).where(
                            and_(
                                Allele.region == region,
                                Allele.position_nt == position_nt,
                                Allele.alt_nt == alt_nt
                            )
                        )
                    ).scalar()
                    if allele is None:
                        allele = Allele(
                            position_nt=position_nt,
                            region=region,
                            alt_nt=alt_nt
                        )
                        session.add(allele)
                    mutation = Mutation(related_sample=sample, related_allele=allele)
                    session.add(mutation)
                    session.commit()


def table_has_rows(table: Type['BaseModel']) -> bool:
    with Session(engine) as session:
        return session.query(table).count() > 0


def main(basedir):
    # unique_srr_file = f'{basedir}/mutations/unique_srr.txt'
    # with open(unique_srr_file, 'r') as f:
    #     uniq_srrs = [l.strip() for l in f.readlines()]
    #
    # samples = [Sample(accession=srr, consent_level=ConsentLevel.public) for srr in uniq_srrs]
    #
    # if not table_has_rows(Sample):
    #     sra_to_sample = insert_samples(samples)
    # else:
    #     print('Samples already has data, skipping...')

    if not table_has_rows(Mutation):
        mutations_files = glob(f'{basedir}/mutdata_complete/*.json')
        parse_and_insert_mutations(mutations_files)
    else:
        print('Mutations already has data, skipping...')

    # I didn't realize there was a combined version and I'm not rewriting the reader
    variants_files = [f'{basedir}/intrahost_dms/combined_variants.tsv']

    if not table_has_rows(IntraHostVariant):
        parse_and_insert_variants(variants_files)
    else:
        print("IntraHostVariants already has data, skipping...")


if __name__ == '__main__':
    basedir = '/home/james/Documents/andersen_lab'
    if len(sys.argv) >= 2:
        basedir = sys.argv[1]
    main(basedir)
