from enum import Enum
from enum import Enum
from typing import Set

import polars as pl

from DB.inserts.alleles import batch_insert_alleles, bulk_insert_new_alleles_skip_existing
from DB.inserts.amino_acid_substitutions import batch_insert_aa_subs
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.mutations import find_or_insert_mutation, batch_insert_mutations
from DB.inserts.samples import find_sample_id_by_accession, batch_find_samples
from DB.inserts.translations import batch_insert_translations
from DB.models import Mutation
from DB.queries.alleles import get_all_alleles_as_pl_df
from utils.constants import StandardColumnNames
from utils.csv_helpers import gff_feature_strip_region_name
from utils.errors import NotFoundError


class MutationsTsvParser(FileParser):

    def __init__(self, filename: str):
        self.filename = filename

    async def parse_and_insert(self):
        debug_info = {
            'skipped_malformed': 0,
            'skipped_sample_not_found': 0
        }
        with open(self.filename, 'r') as f:
            mutations_data = pl.read_csv(f, separator='\t')
            self._verify_header(mutations_data)

            mutations_data = mutations_data.with_columns(
                pl.col(ColNameMapping.region.value).alias(ColNameMapping.region.name),
                pl.col(ColNameMapping.position_nt.value).alias(ColNameMapping.position_nt.name),
                pl.col(ColNameMapping.ref_nt.value).alias(ColNameMapping.ref_nt.name),
                pl.col(ColNameMapping.alt_nt.value).alias(ColNameMapping.alt_nt.name),
                (
                    pl.col(ColNameMapping.gff_feature.value)
                    .map_elements(gff_feature_strip_region_name, return_dtype=pl.String)
                    .alias(ColNameMapping.gff_feature.name)
                ),
                pl.col(ColNameMapping.position_aa.value).alias(ColNameMapping.position_aa.name),
                pl.col(ColNameMapping.ref_aa.value).alias(ColNameMapping.ref_aa.name),
                pl.col(ColNameMapping.alt_aa.value).alias(ColNameMapping.alt_aa.name),
                pl.col(ColNameMapping.ref_codon.value).alias(ColNameMapping.ref_codon.name),
                pl.col(ColNameMapping.alt_codon.value).alias(ColNameMapping.alt_codon.name),
                pl.col(ColNameMapping.accession.value).alias(ColNameMapping.accession.name)
            )


            # Insert alleles and amino subs

            allele_data = mutations_data.select(
                pl.col(ColNameMapping.region.name),
                pl.col(ColNameMapping.position_nt.name),
                pl.col(ColNameMapping.ref_nt.name),
                pl.col(ColNameMapping.alt_nt.name),
            ).unique()


            await bulk_insert_new_alleles_skip_existing(allele_data)

            # todo: insert new alleles and get ids



            # allele_data = await batch_insert_alleles(
            #     allele_data,
            # )

            amino_sub_data = mutations_data.select(
                pl.col(ColNameMapping.gff_feature.name),
                pl.col(ColNameMapping.position_aa.name),
                pl.col(ColNameMapping.ref_aa.name),
                pl.col(ColNameMapping.alt_aa.name),
                pl.col(ColNameMapping.ref_codon.name),
                pl.col(ColNameMapping.alt_codon.name),
            ).unique(
                {ColNameMapping.gff_feature.name, ColNameMapping.position_aa.name, ColNameMapping.alt_aa.name}
            ).drop_nulls()
            amino_sub_data = await batch_insert_aa_subs(
                amino_sub_data
            )

            # join the allele ids back into the original df
            mutations_data = mutations_data.join(
                allele_data,
                on=[
                    ColNameMapping.region.name,
                    ColNameMapping.position_nt.name,
                    ColNameMapping.alt_nt.name
                ],
                how='left'
            )
            mutations_data = mutations_data.join(
                amino_sub_data,
                on=[
                    ColNameMapping.gff_feature.name,
                    ColNameMapping.position_aa.name,
                    ColNameMapping.alt_aa.name
                ],
                how='left'
            )

            translations_data = mutations_data.select(
                pl.col(StandardColumnNames.allele_id),
                pl.col(StandardColumnNames.amino_acid_substitution_id)
            ).unique().drop_nulls()

            await batch_insert_translations(translations_data)

            sample_data = mutations_data.select(
                pl.col(ColNameMapping.accession.name)
            ).unique()
            sample_data = await batch_find_samples(sample_data, accession_name=ColNameMapping.accession.name)

            mutations_data = mutations_data.join(
                sample_data,
                on=ColNameMapping.accession.name,
                how='left'
            )

            mutations_data = mutations_data.filter(
                pl.col(StandardColumnNames.sample_id).is_not_null()
            ).select(
                pl.col(StandardColumnNames.sample_id),
                pl.col(StandardColumnNames.allele_id)
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
