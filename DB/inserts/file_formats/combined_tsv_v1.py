import csv
from csv import DictReader
from enum import Enum

from DB.inserts.alleles import find_or_insert_allele
from DB.inserts.amino_acid_substitutions import find_or_insert_aa_sub
from DB.inserts.samples import find_sample_id_by_accession
from DB.inserts.variants import find_or_insert_variant
from DB.models import Allele, AminoAcidSubstitution, IntraHostVariant
from utils.csv_helpers import value_or_none


# what does a file format need?
# 1. column name mapping
# 2. delimiter
# 3. functions to parse a file and do the insertions.
# 4. Ideally it should be able to tell you about what data it intends to read and insert?


async def insert_from_file(filename: str) -> None:
    # create a csv reader spec'd to the header we're expecting

    with open(filename, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        _verify_header(reader)

        for row in reader:
            try:
                # allele data
                allele_id = await find_or_insert_allele(
                    Allele(
                        region=row[ColNameMapping.region.value],
                        position_nt=int(row[ColNameMapping.position_nt.value]),
                        ref_nt=row[ColNameMapping.ref_nt.value],
                        alt_nt=row[ColNameMapping.alt_nt.value]
                    )
                )

                # amino acid info
                # should either all be present or all be absent
                # we use gff_feature as our canary. If it's present and other values are missing, the db will complain
                gff_feature = value_or_none(row, ColNameMapping.gff_feature.value)
                if gff_feature is not None:
                    await find_or_insert_aa_sub(
                        AminoAcidSubstitution(
                            position_aa=(int(row[ColNameMapping.position_aa.value])),
                            ref_aa=(row[ColNameMapping.ref_aa.value]),
                            alt_aa=(row[ColNameMapping.alt_aa.value]),
                            ref_codon=(row[ColNameMapping.ref_codon.value]),
                            alt_codon=(row[ColNameMapping.alt_codon.value]),
                            allele_id=allele_id
                        )
                    )

                sample_accession = row[ColNameMapping.accession.value]
                sample_id = await find_sample_id_by_accession(sample_accession)

                # variant data
                variant = IntraHostVariant(
                    sample_id=sample_id,
                    allele_id=allele_id,
                    pval=(row[ColNameMapping.pval.value]),
                    ref_dp=(row[ColNameMapping.ref_dp.value]),
                    alt_dp=(row[ColNameMapping.alt_dp.value]),
                    ref_rv=(row[ColNameMapping.ref_rv.value]),
                    alt_rv=(row[ColNameMapping.alt_rv.value]),
                    ref_qual=(row[ColNameMapping.ref_qual.value]),
                    alt_qual=(row[ColNameMapping.alt_qual.value]),
                    pass_qc=(row[ColNameMapping.pass_qc.value]),
                    alt_freq=(row[ColNameMapping.alt_freq.value]),
                    total_dp=(row[ColNameMapping.total_dp.value]),
                )

                _, preexisting = await find_or_insert_variant(variant)

                if preexisting:
                    # todo: proper logging
                    print(
                        f'Warning, tried to insert two variants for the same sample-allele pair, '
                        f'sample: {sample_id}, allele: {allele_id}'
                    )

                # todo: deal with dms values

            except KeyError as e:
                # todo: logging
                print(f'Malformed row in variants: {row}, {str(e)}')


class ColNameMapping(Enum):
    region = 'REGION'
    position_nt = 'POS'
    ref_nt = 'REF'
    alt_nt = 'ALT'

    position_aa = 'POS_AA'
    ref_aa = 'REF_AA'
    alt_aa = 'ALT_AA'
    gff_feature = 'GFF_FEATURE'
    ref_codon = 'REF_CODON'
    alt_codon = 'ALT_CODON'

    accession = 'sra'

    pval = 'PVAL'
    ref_dp = 'REF_DP'
    ref_rv = 'REF_RV'
    ref_qual = 'REF_QUAL'
    alt_dp = 'ALT_DP'
    alt_rv = 'ALT_RV'
    alt_qual = 'ALT_QUAL'
    pass_qc = 'PASS'
    alt_freq = 'ALT_FREQ'
    total_dp = 'TOTAL_DP'

    # todo: in theory the way we plan to store dms data should make this silly,
    #  but it needs to be like this for the moment.
    #  Also: there's a difference between letting the schema be flexible about what data we're storing
    #  and needing the file parsing system to be equally flexible about the data contained in the files it's parsing
    species_sera_escape = 'species sera escape'
    entry_in_293t_cells = 'entry in 293T cells'
    stability = 'stability'
    sa26_usage_increase = 'SA26 usage increase'
    sequential_site = 'sequential_site'
    ref_h1_site = 'reference_H1_site'
    mature_h5_site = 'mature_H5_site'
    ha1_ha2_h5_site = 'HA1_HA2_H5_site'
    # region='region' todo
    nt_changes_to_codon = 'nt changes to codon'


def _verify_header(reader: DictReader) -> None:
    expected_header = [
        ColNameMapping.region.value,
        ColNameMapping.position_nt.value,
        ColNameMapping.ref_nt.value,
        ColNameMapping.alt_nt.value,
        ColNameMapping.ref_dp.value,
        ColNameMapping.ref_rv.value,
        ColNameMapping.ref_qual.value,
        ColNameMapping.alt_dp.value,
        ColNameMapping.alt_rv.value,
        ColNameMapping.alt_qual.value,
        ColNameMapping.alt_freq.value,
        ColNameMapping.total_dp.value,
        ColNameMapping.pval.value,
        ColNameMapping.pass_qc.value,
        ColNameMapping.gff_feature.value,
        ColNameMapping.ref_codon.value,
        ColNameMapping.ref_aa.value,
        ColNameMapping.alt_codon.value,
        ColNameMapping.alt_aa.value,
        ColNameMapping.position_aa.value,
        ColNameMapping.accession.value,
        ColNameMapping.sa26_usage_increase.value,
        ColNameMapping.entry_in_293t_cells.value,
        ColNameMapping.stability.value,
        ColNameMapping.sa26_usage_increase.value,
        ColNameMapping.sequential_site.value,
        ColNameMapping.ref_h1_site.value,
        ColNameMapping.mature_h5_site.value,
        ColNameMapping.ha1_ha2_h5_site.value,
        'region',  # todo
        ColNameMapping.nt_changes_to_codon.value
    ]
    if reader.fieldnames != expected_header:
        raise ValueError('did not find expected header')
