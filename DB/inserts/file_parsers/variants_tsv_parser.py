import csv
from csv import DictReader
from enum import Enum
from typing import Set

from DB.inserts.alleles import find_or_insert_allele
from DB.inserts.amino_acid_substitutions import find_or_insert_aa_sub
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.samples import find_sample_id_by_accession
from DB.inserts.translations import insert_translation
from DB.inserts.variants import find_or_insert_variant
from DB.models import Allele, AminoAcidSubstitution, Translation, IntraHostVariant
from utils.csv_helpers import get_value, int_from_decimal_str, bool_from_str
from utils.errors import NotFoundError


class VariantsTsvParser(FileParser):

    def __init__(self, filename: str):
        self.filename = filename

    async def parse_and_insert(self):
        print_info_interval = 10_000
        debug_info = {
            'skipped_sample_not_found': 0,
            'skipped_malformed': 0,
            'count_new_variants_added': 0
        }

        # accession -> id
        sample_id_cache = dict()

        # (region, position_nt, alt_nt) -> id
        allele_id_cache = dict()

        # (gff_feature, position_aa, alt_aa) -> id
        aas_id_cache = dict()

        # format = accession
        cache_samples_not_found = set()

        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            VariantsTsvParser._verify_header(reader)

            info_last_printed = 0
            for i, row in enumerate(reader):
                try:
                    # a lot of the variants are missing their samples, so let's check for the sample
                    # up front and if the sample is missing we can skip everything else.
                    sample_accession = row[ColNameMapping.accession.value]
                    if sample_accession in cache_samples_not_found:
                        debug_info['skipped_sample_not_found'] += 1
                        continue
                    try:
                        sample_id = sample_id_cache[sample_accession]
                    except KeyError:
                        try:
                            sample_id = await find_sample_id_by_accession(sample_accession)
                            sample_id_cache[sample_accession] = sample_id
                        except NotFoundError:
                            cache_samples_not_found.add(sample_accession)
                            debug_info['skipped_sample_not_found'] += 1
                            continue

                    # allele data
                    region = get_value(row, ColNameMapping.region.value)
                    position_nt = get_value(row, ColNameMapping.position_nt.value, transform=int)
                    alt_nt = get_value(row, ColNameMapping.alt_nt.value)

                    try:
                        allele_id = allele_id_cache[(region, position_nt, alt_nt)]
                    except KeyError:
                        allele_id = await find_or_insert_allele(
                            Allele(
                                region=region,
                                position_nt=position_nt,
                                ref_nt=row[ColNameMapping.ref_nt.value],
                                alt_nt=alt_nt
                            )
                        )
                        allele_id_cache[(region, position_nt, alt_nt)] = allele_id

                    # amino acid info
                    # should either all be present or all be absent
                    # we use gff_feature as our canary.
                    # If it's present and other values are missing, the db will complain
                    gff_feature = get_value(row, ColNameMapping.gff_feature.value, allow_none=True)
                    if gff_feature is not None:
                        position_aa = get_value(
                            row,
                            ColNameMapping.position_aa.value,
                            transform=int_from_decimal_str
                        )
                        alt_aa = (row[ColNameMapping.alt_aa.value])

                        try:
                            aas_id = aas_id_cache[(gff_feature, position_aa, alt_aa)]
                        except KeyError:
                            aas_id = await find_or_insert_aa_sub(
                                AminoAcidSubstitution(
                                    position_aa=position_aa,
                                    ref_aa=(row[ColNameMapping.ref_aa.value]),
                                    alt_aa=alt_aa,
                                    ref_codon=(row[ColNameMapping.ref_codon.value]),
                                    alt_codon=(row[ColNameMapping.alt_codon.value]),
                                    gff_feature=gff_feature
                                )
                            )

                        await insert_translation(
                            Translation(
                                allele_id=allele_id,
                                amino_acid_substitution_id=aas_id
                            )
                        )

                    # variant data
                    variant = IntraHostVariant(
                        sample_id=sample_id,
                        allele_id=allele_id,
                        pval=get_value(row, ColNameMapping.pval.value, transform=float),
                        ref_dp=get_value(row, ColNameMapping.ref_dp.value, transform=int),
                        alt_dp=get_value(row, ColNameMapping.alt_dp.value, transform=int),
                        ref_rv=get_value(row, ColNameMapping.ref_rv.value, transform=int),
                        alt_rv=get_value(row, ColNameMapping.alt_rv.value, transform=int),
                        ref_qual=get_value(row, ColNameMapping.ref_qual.value, transform=int),
                        alt_qual=get_value(row, ColNameMapping.alt_qual.value, transform=int),
                        pass_qc=get_value(row, ColNameMapping.pass_qc.value, transform=bool_from_str),
                        alt_freq=get_value(row, ColNameMapping.alt_freq.value, transform=float),
                        total_dp=get_value(
                            row,
                            ColNameMapping.total_dp.value,
                            transform=int_from_decimal_str
                        ),
                    )

                    _, preexisting = await find_or_insert_variant(variant, upsert=True)

                    if not preexisting:
                        debug_info['count_new_variants_added'] += 1

                    # # todo: proper logging!!
                    # # log debug stats every n lines
                    # if i - info_last_printed >= print_info_interval:
                    #     info_last_printed += print_info_interval
                    #     debug_info['count_samples_not_found'] = len(cache_samples_not_found)
                    #     print(f'{i} lines processed: {debug_info}')

                except (KeyError, ValueError):
                    debug_info['skipped_malformed'] += 1
                    continue

            # print debug info
            debug_info['count_samples_not_found'] = len(cache_samples_not_found)
            print(f'Finished: {debug_info}')

    @classmethod
    def _verify_header(cls, reader: DictReader) -> None:
        required_columns = cls.get_required_column_set()
        if not set(reader.fieldnames) >= required_columns:
            raise ValueError(f'Missing required fields: {required_columns - set(reader.fieldnames)}')

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return {str(cn.value) for cn in {
            ColNameMapping.accession,
            ColNameMapping.region,
            ColNameMapping.position_nt,
            ColNameMapping.ref_nt,
            ColNameMapping.alt_nt,
            ColNameMapping.ref_dp,
            ColNameMapping.ref_rv,
            ColNameMapping.ref_qual,
            ColNameMapping.alt_dp,
            ColNameMapping.alt_rv,
            ColNameMapping.alt_qual,
            ColNameMapping.alt_freq,
            ColNameMapping.total_dp,
            ColNameMapping.pval,
            ColNameMapping.pass_qc,
            ColNameMapping.gff_feature,
            ColNameMapping.ref_codon,
            ColNameMapping.ref_aa,
            ColNameMapping.alt_codon,
            ColNameMapping.alt_aa,
            ColNameMapping.position_aa,
        }}


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
    accession = 'SRA'
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
