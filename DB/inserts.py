import csv
import datetime
import json
import sys
from glob import glob
from typing import List, Type

from sqlalchemy import select, and_, Select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import Sample, Allele, IntraHostVariant, Base, Mutation, AminoAcidSubstitution


def parse_and_insert_samples(samples_file: str):
    with open(samples_file, 'r') as f:
        csvreader = csv.DictReader(f, delimiter=',', quotechar='"')
        for row in csvreader:
            for k, v in row.items():
                if v == '':
                    row[k] = None

            accession = row['Run']
            assay_type = row['Assay Type']
            avg_spot_length = None
            try:
                avg_spot_length = float(row['AvgSpotLen'])
            except TypeError:
                pass
            bases = int(row['Bases'])
            bio_project = row['BioProject']
            bio_sample = row['BioSample']
            bio_sample_model = row['BioSampleModel']
            bytes_ = int(row['Bytes'])
            center_name = row['Center Name']
            collection_date = row['Collection_Date']
            if collection_date == 'missing':
                collection_date = None
            consent_level = row['Consent']
            datastore_filetype = row['DATASTORE filetype']
            datastore_provider = row['DATASTORE provider']
            datastore_region = row['DATASTORE region']
            experiment = row['Experiment']
            geo_loc_name = row['geo_loc_name']
            geo_loc_name_country = row['geo_loc_name_country']
            geo_loc_name_country_continent = row['geo_loc_name_country_continent']
            host = row['Host']
            if host is not None:
                host = host.lower()
            instrument = row['Instrument']
            isolate = row['isolate']
            library_name = row['Library Name']
            library_layout = row['LibraryLayout']
            library_selection = row['LibrarySelection']
            library_source = row['LibrarySource']
            organism = row['Organism']
            platform = row['Platform']
            release_date = datetime.datetime.fromisoformat(row['ReleaseDate'])
            creation_date = datetime.datetime.fromisoformat(row['create_date'])
            version = row['version']
            sample_name = row['Sample Name']
            sra_study = row['SRA Study']
            serotype = row['serotype']
            isolation_source = row['isolation_source']
            bio_sample_accession = row['BioSample Accession']
            is_retracted = row['is_retracted'].lower() == 'true'
            retraction_detected_date = row['retraction_detection_date_utc']
            if retraction_detected_date is not None:
                retraction_detected_date = datetime.datetime.fromisoformat(retraction_detected_date + 'Z')

            sample = Sample(
                accession=accession,
                assay_type=assay_type,
                avg_spot_length=avg_spot_length,
                bases=bases,
                bio_project=bio_project,
                bio_sample=bio_sample,
                bio_sample_model=bio_sample_model,
                bytes=bytes_,
                center_name=center_name,
                collection_date=collection_date,
                consent_level=consent_level,
                datastore_filetype=datastore_filetype,
                datastore_provider=datastore_provider,
                datastore_region=datastore_region,
                experiment=experiment,
                geo_loc_name=geo_loc_name,
                geo_loc_name_country=geo_loc_name_country,
                geo_loc_name_country_continent=geo_loc_name_country_continent,
                host=host,
                instrument=instrument,
                isolate=isolate,
                library_name=library_name,
                library_layout=library_layout,
                library_selection=library_selection,
                library_source=library_source,
                organism=organism,
                platform=platform,
                release_date=release_date,
                creation_date=creation_date,
                version=version,
                sample_name=sample_name,
                sra_study=sra_study,
                serotype=serotype,
                isolation_source=isolation_source,
                bio_sample_accession=bio_sample_accession,
                is_retracted=is_retracted,
                retraction_detected_date=retraction_detected_date
            )

            with Session(engine) as session:
                session.add(sample)
                session.commit()


def find_or_add_sample(session: Session, accession: str) -> Sample:
    sample = session.execute(
        select(Sample).where(Sample.accession == accession)
    ).scalar()
    if sample is None:
        raise NotImplementedError("This scenario is no longer recoverable")
    return sample


def value_or_none(row, key, fn=None):
    try:
        v = row[key]
        if v == '':
            v = None
        elif fn is not None:
            return fn(v)
        return v
    except KeyError:
        return None
    except ValueError:
        return None


def parse_and_insert_variants(files: List[str]):
    errors = []
    for filename in files:
        with (open(filename, 'r') as f):
            csvreader = csv.DictReader(f, delimiter='\t')
            for row in csvreader:

                region_full_text = row['REGION']
                region = region_full_text.split('|')[0]

                ref_nt = row['REF']
                alt_nt = row['ALT']

                position_nt = value_or_none(row, 'POS', int)
                gff_feature = value_or_none(row, 'GFF_FEATURE')

                ref_aa = value_or_none(row, 'REF_AA')

                alt_aa = value_or_none(row, 'ALT_AA')

                position_aa = value_or_none(row, 'POS_AA', lambda x: int(float(x)))

                ref_dp = value_or_none(row, 'REF_DP', int)
                alt_dp = value_or_none(row, 'REF_DP', int)
                total_dp = value_or_none(row, 'TOTAL_DP', int)

                ref_qual = value_or_none(row, 'REF_QUAL', int)
                alt_qual = value_or_none(row, 'ALT_QUAL', int)

                ref_rv = value_or_none(row, 'REF_RV', int)
                alt_rv = value_or_none(row, 'ALT_RV', int)

                pval = value_or_none(row, 'PVAL', float)

                pass_qc = row['PASS'].lower() == 'true'

                alt_freq = value_or_none(row, 'ALT_FREQ', float)

                ref_codon = value_or_none(row, 'REF_CODON')
                alt_codon = value_or_none(row, 'ALT_CODON')

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
                                ref_nt=ref_nt,
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
                                    alt_aa=alt_aa,
                                    ref_codon=ref_codon,
                                    alt_codon=alt_codon
                                )

                            aa_sub.allele_id = allele.id
                            session.add(aa_sub)

                        sample = find_or_add_sample(session, row['sra'])

                        variant = session.execute(
                            Select(IntraHostVariant).where(
                                and_(
                                    IntraHostVariant.allele_id == allele.id,
                                    IntraHostVariant.sample_id == sample.id
                                )
                            )
                        ).scalar()

                        if variant is None:
                            variant = IntraHostVariant(
                                sample_id=sample.id,
                                allele_id=allele.id,
                                ref_dp=ref_dp,
                                alt_dp=alt_dp,
                                alt_freq=alt_freq,
                                total_dp=total_dp,
                                ref_qual=ref_qual,
                                alt_qual=alt_qual,
                                ref_rv=ref_rv,
                                alt_rv=alt_rv,
                                pval=pval,
                                pass_qc=pass_qc
                            )
                            session.add(variant)
                            session.commit()

                    except IntegrityError as e:
                        errors.append(e)
    print(f'Variants done with {len(errors)} errors')


def parse_and_insert_mutations(mutations_files):
    errors = []

    for file in mutations_files:
        with open(file, 'r') as f:
            for row in json.load(f):
                accession = row['sra']
                region = row['region']
                position_nt = int(row['pos'])
                ref_nt = row['ref']
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
                                ref_nt=ref_nt,
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
                            ref_codon = aa_data['ref_codon']
                            alt_codon = aa_data['alt_codon']

                            aa_sub = session.execute(
                                select(AminoAcidSubstitution).where(
                                    and_(
                                        AminoAcidSubstitution.gff_feature == gff_feature,
                                        AminoAcidSubstitution.alt_aa == alt_aa,
                                        AminoAcidSubstitution.position_aa == position_aa
                                    )
                                )
                            ).scalar()
                            if aa_sub is None:
                                aa_sub = AminoAcidSubstitution(
                                    gff_feature=gff_feature,
                                    position_aa=position_aa,
                                    ref_aa=ref_aa,
                                    alt_aa=alt_aa,
                                    ref_codon=ref_codon,
                                    alt_codon=alt_codon
                                )
                                aa_sub.allele_id = allele.id

                        mutation = Mutation(sample_id=sample.id, allele_id=allele.id)
                        session.add(mutation)
                        session.commit()
                    except IntegrityError as e:
                        errors.append(e)
    print(f'Mutations done with {len(errors)} errors')


def table_has_rows(table: Type['Base']) -> bool:
    with Session(engine) as session:
        return session.query(table).count() > 0


def main(basedir):
    start = datetime.datetime.now()
    print(f'start at {start}')
    samples_file = f'{basedir}/SraRunTable_automated.csv'

    if not table_has_rows(Sample):
        parse_and_insert_samples(samples_file)
        print('samples done')
    else:
        print('Samples already has data, skipping...')
    samples_done = datetime.datetime.now()
    print(f'Samples took: {samples_done - start}')

    # I didn't realize there was a combined version and I'm not rewriting the reader
    variants_files = [f'{basedir}/intrahost_dms/combined_variants.tsv']

    if not table_has_rows(IntraHostVariant):
        parse_and_insert_variants(variants_files)
    else:
        print("IntraHostVariants already has data, skipping...")

    variants_done = datetime.datetime.now()
    print(f'Variants took: {variants_done - samples_done}')

    if not table_has_rows(Mutation):
        mutations_files = glob(f'{basedir}/mutdata_complete/*.json')
        parse_and_insert_mutations(mutations_files)
    else:
        print('Mutations already has data, skipping...')

    mutations_done = datetime.datetime.now()
    print(f'Mutations took: {mutations_done - variants_done}')

    end = datetime.datetime.now()
    print(f'Total elapsed: {end - start}')

if __name__ == '__main__':
    basedir_ = '/home/james/Documents/andersen_lab/bird_flu_db/test_data'
    if len(sys.argv) >= 2:
        basedir_ = sys.argv[1]
    main(basedir_)
