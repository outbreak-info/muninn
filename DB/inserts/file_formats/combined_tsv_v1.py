import csv
import json
from csv import DictReader
from datetime import datetime
from enum import Enum
from typing import Dict

from DB.inserts.alleles import find_or_insert_allele
from DB.inserts.amino_acid_substitutions import find_or_insert_aa_sub
from DB.inserts.file_formats.file_format import FileFormat
from DB.inserts.phenotype_measurement_results import insert_pheno_measurement_result
from DB.inserts.phenotype_metrics import find_or_insert_metric
from DB.inserts.samples import find_sample_id_by_accession
from DB.inserts.translations import insert_translation
from DB.inserts.variants import find_or_insert_variant
from DB.models import Allele, AminoAcidSubstitution, IntraHostVariant, Translation, PhenotypeMetric, \
    PhenotypeMeasurementResult
from utils.csv_helpers import get_value, bool_from_str, int_from_decimal_str
from utils.errors import NotFoundError


class CombinedTsvV1(FileFormat):
    # todo: this is super hacky
    # metric name -> id
    _phenotype_metric_cache = dict()

    # accession -> id
    _sample_id_cache = dict()

    # (region, position_nt, alt_nt) -> id
    _allele_id_cache = dict()

    # (gff_feature, position_aa, alt_aa) -> id
    _aas_id_cache = dict()

    @classmethod
    async def insert_from_file(cls, filename: str) -> None:
        # format = accession -> effected variant count
        cache_samples_not_found = dict()
        # format = (sample_id, allele_id) -> times seen (ie 2 when the first duplicate is seen)
        debug_duplicate_variants = dict()
        with open(filename, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            CombinedTsvV1._verify_header(reader)

            for i, row in enumerate(reader):
                try:
                    # a lot of the variants are missing their samples, so let's check for the sample
                    # up front and if the sample is missing we can skip everything else.
                    sample_accession = row[cls.ColNameMapping.accession.value]
                    if sample_accession in cache_samples_not_found.keys():
                        cache_samples_not_found[sample_accession] += 1
                        continue
                    try:
                        sample_id = cls._sample_id_cache[sample_accession]
                    except KeyError:
                        try:
                            sample_id = await find_sample_id_by_accession(sample_accession)
                            cls._sample_id_cache[sample_accession] = sample_id
                        except NotFoundError:
                            # todo: proper logging
                            try:
                                cache_samples_not_found[sample_accession] += 1
                            except KeyError:
                                cache_samples_not_found[sample_accession] = 1
                            continue

                    # allele data
                    region = get_value(row, cls.ColNameMapping.region.value)
                    position_nt = get_value(row, cls.ColNameMapping.position_nt.value, transform=int)
                    alt_nt = get_value(row, cls.ColNameMapping.alt_nt.value)

                    try:
                        allele_id = cls._allele_id_cache[(region, position_nt, alt_nt)]
                    except KeyError:
                        allele_id = await find_or_insert_allele(
                            Allele(
                                region=region,
                                position_nt=position_nt,
                                ref_nt=row[cls.ColNameMapping.ref_nt.value],
                                alt_nt=alt_nt
                            )
                        )
                        cls._allele_id_cache[(region, position_nt, alt_nt)] = allele_id

                    # amino acid info
                    # should either all be present or all be absent
                    # we use gff_feature as our canary.
                    # If it's present and other values are missing, the db will complain
                    gff_feature = get_value(row, cls.ColNameMapping.gff_feature.value, allow_none=True)
                    if gff_feature is not None:
                        position_aa = get_value(
                            row,
                            cls.ColNameMapping.position_aa.value,
                            transform=int_from_decimal_str
                        )
                        alt_aa = (row[cls.ColNameMapping.alt_aa.value])

                        try:
                            aas_id = cls._aas_id_cache[(gff_feature, position_aa, alt_aa)]
                        except KeyError:
                            aas_id = await find_or_insert_aa_sub(
                                AminoAcidSubstitution(
                                    position_aa=position_aa,
                                    ref_aa=(row[cls.ColNameMapping.ref_aa.value]),
                                    alt_aa=alt_aa,
                                    ref_codon=(row[cls.ColNameMapping.ref_codon.value]),
                                    alt_codon=(row[cls.ColNameMapping.alt_codon.value]),
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
                        pval=get_value(row, cls.ColNameMapping.pval.value, transform=float),
                        ref_dp=get_value(row, cls.ColNameMapping.ref_dp.value, transform=int),
                        alt_dp=get_value(row, cls.ColNameMapping.alt_dp.value, transform=int),
                        ref_rv=get_value(row, cls.ColNameMapping.ref_rv.value, transform=int),
                        alt_rv=get_value(row, cls.ColNameMapping.alt_rv.value, transform=int),
                        ref_qual=get_value(row, cls.ColNameMapping.ref_qual.value, transform=int),
                        alt_qual=get_value(row, cls.ColNameMapping.alt_qual.value, transform=int),
                        pass_qc=get_value(row, cls.ColNameMapping.pass_qc.value, transform=bool_from_str),
                        alt_freq=get_value(row, cls.ColNameMapping.alt_freq.value, transform=float),
                        total_dp=get_value(row, cls.ColNameMapping.total_dp.value, transform=int_from_decimal_str),
                    )

                    _, preexisting = await find_or_insert_variant(variant)

                    if preexisting:
                        # todo: proper logging
                        try:
                            debug_duplicate_variants[(sample_id, allele_id)] += 1
                        except KeyError:
                            debug_duplicate_variants[(sample_id, allele_id)] = 2

                    # todo: proper logging!!
                    # log debug stats every n lines
                    if i % 10_000 == 0:
                        print(f'{datetime.now()} : {i} lines processed')
                        with open('/tmp/samples_not_found.log.json', 'w+') as lf:
                            json.dump(cache_samples_not_found, lf, indent=4)
                        with open('/tmp/duplicate_variants.log.json', 'w+') as lf:
                            json.dump({str(k): v for k, v in debug_duplicate_variants.items()}, lf, indent=4)

                except (KeyError, ValueError) as e:
                    # todo: logging
                    print(f'Malformed row in variants: {row}, {str(e)}')

            # reset cache
            cls._phenotype_metric_cache = dict()
            cls._aas_id_cache = dict()
            cls._sample_id_cache = dict()

            # todo: proper logging
            with open('/tmp/samples_not_found.log.json', 'w+') as lf:
                json.dump(cache_samples_not_found, lf, indent=4)
            with open('/tmp/duplicate_variants.log.json', 'w+') as lf:
                json.dump({str(k): v for k, v in debug_duplicate_variants.items()}, lf, indent=4)

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

    @classmethod
    def _verify_header(cls, reader: DictReader) -> None:

        required_columns = {cn.value for cn in {
            cls.ColNameMapping.accession,
            cls.ColNameMapping.region,
            cls.ColNameMapping.position_nt,
            cls.ColNameMapping.ref_nt,
            cls.ColNameMapping.alt_nt,
            cls.ColNameMapping.ref_dp,
            cls.ColNameMapping.ref_rv,
            cls.ColNameMapping.ref_qual,
            cls.ColNameMapping.alt_dp,
            cls.ColNameMapping.alt_rv,
            cls.ColNameMapping.alt_qual,
            cls.ColNameMapping.alt_freq,
            cls.ColNameMapping.total_dp,
            cls.ColNameMapping.pval,
            cls.ColNameMapping.pass_qc,
            cls.ColNameMapping.gff_feature,
            cls.ColNameMapping.ref_codon,
            cls.ColNameMapping.ref_aa,
            cls.ColNameMapping.alt_codon,
            cls.ColNameMapping.alt_aa,
            cls.ColNameMapping.position_aa,

        }}

        if not set(reader.fieldnames) >= required_columns:
            raise ValueError(f'Missing required fields: {required_columns - set(reader.fieldnames)}')
