import csv
import json
import sys
from glob import glob
from typing import List, Type

from sqlalchemy import select, and_, Select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from DB.engine import engine

from DB.models import Sample, Allele, IntraHostVariant, Base, Mutation, AminoAcidSubstitution


def insert_samples(data: List['Sample']):
    with Session(engine) as session:
        for sample in data:
            session.add(sample)

        session.flush()
        sra_to_sample = {s.accession: s for s in data}
        session.commit()
    return sra_to_sample


def find_or_add_sample(session: Session, accession: str) -> Sample:
    sample = session.execute(
        select(Sample).where(Sample.accession == accession)
    ).scalar()
    if sample is None:
        sample = Sample(accession=accession, consent_level='public')
        session.add(sample)
        session.commit()
        session.refresh(sample)
    return sample


def parse_and_insert_variants(files: List[str]):
    for filename in files:
        with (open(filename, 'r') as f):
            csvreader = csv.DictReader(f, delimiter='\t')
            for row in csvreader:

                region_full_text = row['REGION']
                region = region_full_text.split('|')[0]

                alt_nt = row['ALT']

                position_nt = int(row['POS'])
                gff_feature = row['GFF_FEATURE']

                # todo: need a way to handle * and - aminos better
                ref_aa = None
                try:
                    ref_aa = row['REF_AA']
                except KeyError:
                    pass

                alt_aa = None
                try:
                    alt_aa = row['ALT_AA']
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

                query_allele = select(Allele).where(
                    and_(
                        Allele.region == region,
                        Allele.position_nt == position_nt,
                        Allele.alt_nt == alt_nt
                    )
                )

                # for now just use gff_feature to tell if aa sub info is present
                aa_info_present = gff_feature is not None

                with(Session(engine)) as session:
                    try:

                        allele = session.execute(query_allele).scalar()
                        if allele is None:
                            allele = Allele(
                                position_nt=position_nt,
                                alt_nt=alt_nt,
                                region=region
                            )
                            session.add(allele)
                            session.commit()
                            session.refresh(allele)

                        if aa_info_present:
                            aa_sub = session.execute(
                                select(AminoAcidSubstitution).where(
                                    and_(
                                        AminoAcidSubstitution.gff_feature == gff_feature,
                                        AminoAcidSubstitution.alt_aa == alt_aa,
                                        AminoAcidSubstitution.ref_aa == ref_aa,
                                        AminoAcidSubstitution.position_aa == position_aa
                                    )
                                )
                            ).scalar()
                            if aa_sub is None:
                                aa_sub = AminoAcidSubstitution(
                                    gff_feature=gff_feature,
                                    position_aa=position_aa,
                                    ref_aa=ref_aa,
                                    alt_aa=alt_aa
                                )

                            aa_sub.allele_id = allele.id
                            session.add(aa_sub)


                        sample = find_or_add_sample(session, row['sra'])

                        variant = session.execute(
                            Select(IntraHostVariant).where(and_(
                                IntraHostVariant.allele_id == allele.id,
                                IntraHostVariant.sample_id == sample.id
                            ))
                        ).scalar()

                        if variant is None:
                            variant = IntraHostVariant(
                                sample_id=sample.id,
                                allele_id=allele.id,
                                ref_dp=int(row['REF_DP']),
                                alt_dp=int(row['ALT_DP']),
                                alt_freq=float(row['ALT_FREQ'])
                            )
                            session.add(variant)
                            session.commit()

                    except IntegrityError as e:
                        print(e)


def parse_and_insert_mutations(mutations_files):
    for file in mutations_files:
        with open(file, 'r') as f:
            for row in json.load(f):
                accession = row['sra']
                region = row['region']
                position_nt = int(row['pos'])
                alt_nt = row['alt']

                with Session(engine) as session:
                    try:
                        sample = find_or_add_sample(session, accession)

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
                            session.commit()
                            session.refresh(allele)

                        for aa_data in row['aa_mutations']:
                            gff_feature = aa_data['GFF_FEATURE']
                            ref_aa = aa_data['ref_aa']
                            alt_aa = aa_data['alt_aa']
                            position_aa = aa_data['pos_aa']

                            aa_sub = session.execute(
                                select(AminoAcidSubstitution).where(
                                    and_(
                                        AminoAcidSubstitution.gff_feature == gff_feature,
                                        AminoAcidSubstitution.alt_aa == alt_aa,
                                        AminoAcidSubstitution.ref_aa == ref_aa,
                                        AminoAcidSubstitution.position_aa == position_aa
                                    )
                                )
                            ).scalar()
                            if aa_sub is None:
                                aa_sub = AminoAcidSubstitution(
                                    gff_feature=gff_feature,
                                    position_aa=position_aa,
                                    ref_aa=ref_aa,
                                    alt_aa=alt_aa
                                )
                                aa_sub.allele_id = allele.id

                        mutation = Mutation(sample_id=sample.id, allele_id=allele.id)
                        session.add(mutation)
                        session.commit()
                    except IntegrityError as e:
                        print(e)


def table_has_rows(table: Type['Base']) -> bool:
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
