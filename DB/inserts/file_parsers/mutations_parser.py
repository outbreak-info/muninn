from enum import Enum
from enum import Enum
from typing import Set

import polars as pl

from DB.inserts.alleles import batch_insert_alleles
from DB.inserts.amino_acid_substitutions import batch_insert_aa_subs
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.mutations import find_or_insert_mutation, batch_insert_mutations
from DB.inserts.samples import find_sample_id_by_accession, batch_find_samples
from DB.inserts.translations import batch_insert_translations
from DB.models import Mutation
from utils.errors import NotFoundError


class MutationsTsvParser(FileParser):

    def __init__(self, filename: str):
        self.filename = filename

    async def parse_and_insert(self):
        debug_info = {
            'skipped_malformed': 0,
            'skipped_sample_not_found': 0
        }

        cache_samples = dict()
        cache_accessions_not_found = set()
        with open(self.filename, 'r') as f:
            reader = pl.read_csv(f, separator='\t')
            self._verify_header(reader)

            # Insert alleles and amino subs

            allele_data = reader.select(
                pl.col(ColNameMapping.region.value),
                pl.col(ColNameMapping.position_nt.value),
                pl.col(ColNameMapping.ref_nt.value),
                pl.col(ColNameMapping.alt_nt.value),
            ).unique()
            allele_data = await batch_insert_alleles(
                allele_data,
                region_name=ColNameMapping.region.value,
                position_nt_name=ColNameMapping.position_nt.value,
                ref_nt_name=ColNameMapping.ref_nt.value,
                alt_nt_name=ColNameMapping.alt_nt.value
            )

            amino_sub_data = reader.select(
                pl.col(ColNameMapping.gff_feature.value),
                pl.col(ColNameMapping.position_aa.value),
                pl.col(ColNameMapping.ref_aa.value),
                pl.col(ColNameMapping.alt_aa.value),
                pl.col(ColNameMapping.ref_codon.value),
                pl.col(ColNameMapping.alt_codon.value),
            ).unique(
                {ColNameMapping.gff_feature.value, ColNameMapping.position_aa.value, ColNameMapping.alt_aa.value}
            ).drop_nulls()
            amino_sub_data = await batch_insert_aa_subs(
                amino_sub_data,
                gff_feature_name=ColNameMapping.gff_feature.value,
                position_aa_name=ColNameMapping.position_aa.value,
                ref_aa_name=ColNameMapping.ref_aa.value,
                alt_aa_name=ColNameMapping.alt_aa.value,
                ref_codon_name=ColNameMapping.ref_codon.value,
                alt_codon_name=ColNameMapping.alt_codon.value,
            )

            # join the allele ids back into the original df
            reader = reader.join(
                allele_data,
                on=[
                    ColNameMapping.region.value,
                    ColNameMapping.position_nt.value,
                    ColNameMapping.alt_nt.value
                ],
                how='left'
            )
            reader = reader.join(
                amino_sub_data,
                on=[
                    ColNameMapping.gff_feature.value,
                    ColNameMapping.position_aa.value,
                    ColNameMapping.alt_aa.value
                ],
                how='left'
            )

            translations_data = reader.select(
                pl.col('allele_id'),
                pl.col('amino_acid_substitution_id')
            ).unique().drop_nulls()

            await batch_insert_translations(translations_data)

            sample_data = reader.select(
                pl.col(ColNameMapping.accession.value)
            ).unique()
            sample_data = await batch_find_samples(sample_data, accession_name=ColNameMapping.accession.value)

            reader = reader.join(
                sample_data,
                on=ColNameMapping.accession.value,
                how='left'
            )

            mutations_data = reader.filter(
                pl.col('sample_id').is_not_null()
            ).select(
                pl.col('sample_id'),
                pl.col('allele_id')
            ).unique()

            await batch_insert_mutations(mutations_data)

            # for row in reader.iter_rows(named=True):
            #     try:
            #         if row['sample_id'] is None:
            #             debug_info['skipped_sample_not_found'] += 1
            #             continue
            #
            #         mutation = Mutation(
            #             sample_id=row['sample_id'],
            #             allele_id=row['allele_id']
            #         )
            #         await find_or_insert_mutation(mutation)
            #
            #     except ValueError:
            #         debug_info['skipped_malformed'] += 1
        print(debug_info)

    @classmethod
    def _verify_header(cls, reader: pl.DataFrame):
        required_columns = cls.get_required_column_set()
        diff = required_columns - set(reader.columns)
        if not len(diff) == 0:
            raise ValueError(f'Not all required columns are present, missing: {diff}')

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return {str(cn.value) for cn in {
            ColNameMapping.accession,
            ColNameMapping.region,
            ColNameMapping.position_nt,
            ColNameMapping.ref_nt,
            ColNameMapping.alt_nt,
            ColNameMapping.gff_feature,
            ColNameMapping.ref_codon,
            ColNameMapping.alt_codon,
            ColNameMapping.ref_aa,
            ColNameMapping.alt_aa,
            ColNameMapping.position_aa,
        }}


class ColNameMapping(Enum):
    accession = 'sra'
    position_nt = 'pos'
    ref_nt = 'ref'
    alt_nt = 'alt'
    region = 'region'
    gff_feature = 'GFF_FEATURE'
    ref_codon = 'ref_codon'
    alt_codon = 'alt_codon'
    ref_aa = 'ref_aa'
    alt_aa = 'alt_aa'
    position_aa = 'pos_aa'
