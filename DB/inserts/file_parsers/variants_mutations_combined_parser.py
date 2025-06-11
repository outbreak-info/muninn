import csv
import os.path
from typing import List, Set

import polars as pl

from DB.inserts.alleles import copy_insert_alleles
from DB.inserts.amino_acid_substitutions import copy_insert_aa_subs
from DB.inserts.file_parsers import variants_tsv_parser, mutations_parser
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.file_parsers.mutations_parser import MutationsTsvParser
from DB.inserts.file_parsers.variants_tsv_parser import VariantsTsvParser
from DB.inserts.mutations import copy_insert_mutations
from DB.inserts.translations import copy_insert_translations
from DB.inserts.variants import batch_upsert_variants, copy_insert_variants
from DB.queries.alleles import get_all_alleles_as_pl_df
from DB.queries.amino_acid_substitutions import get_all_amino_acid_subs_as_pl_df
from DB.queries.mutations import get_all_mutations_as_pl_df
from DB.queries.samples import get_samples_accession_and_id_as_pl_df
from DB.queries.translations import get_all_translations_as_pl_df
from DB.queries.variants import get_all_variants_as_pl_df
from utils.constants import StandardColumnNames
from utils.csv_helpers import gff_feature_strip_region_name


class VariantsMutationsCombinedParser(FileParser):

    def __init__(self, variants_filename: str, mutations_filename: str):
        self.variants_filename = variants_filename
        self.mutations_filename = mutations_filename
        self.delimiter = '\t'
        try:
            self._verify_headers()
        except ValueError:
            # Swap arguments and try again
            self.variants_filename = mutations_filename
            self.mutations_filename = variants_filename
            self._verify_headers()

    async def parse_and_insert(self):
        debug_info = {
            'count_alleles_added': 'unset',
            'count_amino_subs_added': 'unset',
            'count_translations_added': 'unset',
            'count_mutations_added': 'unset',
            'count_existing_variants': 'unset',
            'count_variants_added': 'unset'
        }

        #  1. read vars and muts
        variants_input: pl.LazyFrame = await self._scan_variants()
        mutations_input: pl.LazyFrame = await self._scan_mutations()

        #  2. Get accession -> id mapping from db
        #  3. Filter out vars and muts with accessions missing from db
        variants_with_samples, mutations_with_samples = await (
            VariantsMutationsCombinedParser._get_vars_and_muts_for_existing_samples(variants_input, mutations_input)
        )

        #  4. split out and combine alleles
        #  5. filter out existing alleles
        #  6. Insert new alleles via copy
        debug_info['count_alleles_added'] = await VariantsMutationsCombinedParser._insert_new_alleles(
            variants_with_samples,
            mutations_with_samples,
        )
        print(f'alleles added: {debug_info}')

        #  7. split out and combine amino acid subs
        #  8. filter out existing AA subs
        #  9. insert new aa subs via copy
        debug_info['count_amino_subs_added'] = await VariantsMutationsCombinedParser._insert_new_amino_acid_subs(
            variants_with_samples,
            mutations_with_samples
        )
        print(f'amino subs added: {debug_info}')

        # 10. Get new allele / AAS ids and join back into vars and muts
        variants_finished, mutations_finished = await (
            VariantsMutationsCombinedParser
            ._join_alleles_and_amino_subs_into_vars_and_muts(variants_with_samples, mutations_with_samples)
        )

        # 11. Split out and insert new translations from vars and muts
        debug_info['count_translations_added'] = await (
            VariantsMutationsCombinedParser
            ._insert_new_translations(variants_finished, mutations_finished)
        )

        print(f'translations added: {debug_info}')

        # 12. Filter out existing mutations (updates not allowed)
        # 13. insert new mutations via copy
        debug_info['count_mutations_added'] = await (
            VariantsMutationsCombinedParser._insert_new_mutations(mutations_finished)
        )
        print(f'mutations added: {debug_info}')

        # 14. Separate new and existing variants
        # 15. Insert new variants via copy
        # 16. Update existing variants (new bulk process for this?)
        existing_variants = await get_all_variants_as_pl_df()
        debug_info['count_variants_added'] = await (
            VariantsMutationsCombinedParser._insert_new_variants(variants_finished, existing_variants)
        )
        debug_info['count_preexisting_variants'] = await (
            VariantsMutationsCombinedParser._update_existing_variants(variants_finished, existing_variants)
        )

        print(f'variants added / updated: {debug_info}')

    async def _scan_variants(self):
        def variants_colname_mapping(cns: List[str]) -> List[str]:
            mapped_names = []
            for cn in cns:
                try:
                    mapped_names.append(variants_tsv_parser.ColNameMapping(cn).name)
                except ValueError:
                    mapped_names.append(cn)
            return mapped_names

        variants_input: pl.LazyFrame = (pl.scan_csv(
            self.variants_filename,
            with_column_names=variants_colname_mapping,
            separator=self.delimiter
        ))
        return VariantsMutationsCombinedParser._clean_and_unique_variants_and_mutations(variants_input)

    async def _scan_mutations(self):
        def mutations_colname_mapping(cns: List[str]) -> List[str]:
            mapped_names = []
            for cn in cns:
                try:
                    mapped_names.append(mutations_parser.ColNameMapping(cn).name)
                except ValueError:
                    mapped_names.append(cn)
            return mapped_names

        mutations_input: pl.LazyFrame = pl.scan_csv(
            self.mutations_filename,
            with_column_names=mutations_colname_mapping,
            separator=self.delimiter
        )
        return VariantsMutationsCombinedParser._clean_and_unique_variants_and_mutations(mutations_input)

    @staticmethod
    def _clean_and_unique_variants_and_mutations(raw: pl.LazyFrame) -> pl.LazyFrame:
        return raw.with_columns(
            pl.col(StandardColumnNames.ref_nt).str.to_uppercase().alias(StandardColumnNames.ref_nt),
            pl.col(StandardColumnNames.alt_nt).str.to_uppercase().alias(StandardColumnNames.alt_nt),
            pl.col(StandardColumnNames.ref_aa).str.to_uppercase().alias(StandardColumnNames.ref_aa),
            pl.col(StandardColumnNames.alt_aa).str.to_uppercase().alias(StandardColumnNames.alt_aa),
            pl.col(StandardColumnNames.ref_codon).str.to_uppercase().alias(StandardColumnNames.ref_codon),
            pl.col(StandardColumnNames.alt_codon).str.to_uppercase().alias(StandardColumnNames.alt_codon),
            (
                pl.col(StandardColumnNames.gff_feature)
                .map_elements(gff_feature_strip_region_name, return_dtype=pl.String)
                .alias(StandardColumnNames.gff_feature)

            ),
            pl.col(StandardColumnNames.position_aa).cast(pl.Int64)
        ).unique(
            [
                StandardColumnNames.accession,
                StandardColumnNames.region,
                StandardColumnNames.position_nt,
                StandardColumnNames.alt_nt
            ]
        )

    @staticmethod
    async def _get_vars_and_muts_for_existing_samples(
        variants_input: pl.LazyFrame,
        mutations_input: pl.LazyFrame
    ) -> (pl.LazyFrame, pl.LazyFrame):
        #  2. Get accession -> id mapping from db
        existing_samples: pl.DataFrame = await get_samples_accession_and_id_as_pl_df()

        #  3. Filter out vars and muts with accessions missing from db
        variants_with_samples = variants_input.join(
            existing_samples.lazy(),
            on=StandardColumnNames.accession,
            how='inner'
        )

        mutations_with_samples = mutations_input.join(
            existing_samples.lazy(),
            on=StandardColumnNames.accession,
            how='inner'
        )

        return variants_with_samples, mutations_with_samples

    @staticmethod
    async def _insert_new_alleles(
        variants_with_samples: pl.LazyFrame,
        mutations_with_samples: pl.LazyFrame,
    ) -> str:
        #  4. split out and combine alleles
        allele_cols = {
            StandardColumnNames.region,
            StandardColumnNames.position_nt,
            StandardColumnNames.ref_nt,
            StandardColumnNames.alt_nt,
        }

        alleles = pl.concat(
            [
                variants_with_samples.select(allele_cols),
                mutations_with_samples.select(allele_cols)
            ]
        ).unique(
            # including ref here to allow checking for conflicts on ref
            allele_cols
        )

        #  5. filter out existing alleles
        existing_alleles: pl.DataFrame = await get_all_alleles_as_pl_df()

        new_alleles = alleles.join(
            existing_alleles.lazy(),
            on=[
                StandardColumnNames.region,
                StandardColumnNames.position_nt,
                StandardColumnNames.alt_nt,
            ],
            how='anti'
        ).filter(  # filter out deletions with no NT data, formatted as +N
            pl.col(StandardColumnNames.alt_nt).str.count_matches(r'\+\d+') == 0
        ).unique(  # this is required b/c we're including ref in the first unique above.
            [
                pl.col(StandardColumnNames.region),
                pl.col(StandardColumnNames.position_nt),
                pl.col(StandardColumnNames.alt_nt)
            ]
        )

        # Check for ref conflicts
        ref_conflicts = (pl.concat(
            [
                existing_alleles.select(allele_cols).lazy(),
                alleles
            ]
        )
        .unique()
        .group_by(
            pl.col(StandardColumnNames.region),
            pl.col(StandardColumnNames.position_nt),
            pl.col(StandardColumnNames.alt_nt)
        )
        .len()
        .filter(pl.col('len') > 1)
        .select(
            pl.col(StandardColumnNames.region),
            pl.col(StandardColumnNames.position_nt),
            pl.col(StandardColumnNames.alt_nt)
        )).collect()

        if len(ref_conflicts) > 0:
            output_file = '/tmp/allele_ref_conflicts.csv'
            print(
                f'WARNING: in alleles, found {len(ref_conflicts)} positions with conflicting values for ref_nt. '
                f'Written to {output_file}'
            )
            ref_conflicts.write_csv(output_file)

        # ref conflicts have already been filtered out of new_alleles above, so we are good to insert

        #  6. Insert new alleles via copy
        return await copy_insert_alleles(new_alleles.collect())

    @staticmethod
    async def _insert_new_amino_acid_subs(
        variants_with_samples: pl.LazyFrame,
        mutations_with_samples: pl.LazyFrame,
    ) -> str:
        #  7. split out and combine amino acid subs
        amino_sub_cols = {
            StandardColumnNames.gff_feature,
            StandardColumnNames.position_aa,
            StandardColumnNames.ref_aa,
            StandardColumnNames.alt_aa,
            StandardColumnNames.ref_codon,
            StandardColumnNames.alt_codon
        }
        amino_subs_v = variants_with_samples.select(amino_sub_cols)
        amino_subs_m = mutations_with_samples.select(amino_sub_cols)

        amino_subs = pl.concat([amino_subs_v, amino_subs_m]).drop_nulls().unique(
            {
                StandardColumnNames.gff_feature,
                StandardColumnNames.position_aa,
                StandardColumnNames.alt_aa,
                StandardColumnNames.ref_aa  # Keep ref here to allow checking for conflicts
            }
        )

        #  8. filter out existing AA subs
        existing_amino_subs = await get_all_amino_acid_subs_as_pl_df()
        new_amino_subs = amino_subs.join(
            existing_amino_subs.lazy(),
            on=[
                StandardColumnNames.gff_feature,
                StandardColumnNames.position_aa,
                StandardColumnNames.alt_aa,
            ],
            how='anti'
        ).unique(  # this is required b/c we're including ref in the first unique above.
            [
                pl.col(StandardColumnNames.gff_feature),
                pl.col(StandardColumnNames.position_aa),
                pl.col(StandardColumnNames.alt_aa)
            ]
        )

        # Check for conflicts on ref_aa
        ref_conflicts = (pl.concat(
            [
                existing_amino_subs.select(
                    {
                        StandardColumnNames.gff_feature,
                        StandardColumnNames.position_aa,
                        StandardColumnNames.ref_aa,
                        StandardColumnNames.alt_aa,
                    }
                ).lazy(),
                amino_subs.select(
                    {
                        StandardColumnNames.gff_feature,
                        StandardColumnNames.position_aa,
                        StandardColumnNames.ref_aa,
                        StandardColumnNames.alt_aa,
                    }
                )
            ]
        )
        .unique()
        .group_by(
            pl.col(StandardColumnNames.gff_feature),
            pl.col(StandardColumnNames.position_aa),
            pl.col(StandardColumnNames.alt_aa)
        )
        .len()
        .filter(pl.col('len') > 1)
        .select(
            pl.col(StandardColumnNames.gff_feature),
            pl.col(StandardColumnNames.position_aa),
            pl.col(StandardColumnNames.alt_aa)
        )).collect()

        if len(ref_conflicts) > 0:
            output_file = '/tmp/amino_sub_ref_conflicts.csv'
            print(
                f'WARNING: in amino acid subs, found {len(ref_conflicts)} positions with conflicting values for ref_aa. '
                f'Written to {output_file}'
            )
            ref_conflicts.write_csv(output_file)

        # ref conflicts have already been filtered out of new_amino_subs above, so we are good to insert

        #  9. insert new aa subs via copy
        return await copy_insert_aa_subs(new_amino_subs.collect())

    @staticmethod
    async def _join_alleles_and_amino_subs_into_vars_and_muts(
        variants_with_samples: pl.LazyFrame,
        mutations_with_samples: pl.LazyFrame
    ) -> (pl.LazyFrame, pl.LazyFrame):
        # 10. Get new allele / AAS ids and join back into vars and muts
        existing_alleles = await get_all_alleles_as_pl_df()
        existing_amino_subs = await get_all_amino_acid_subs_as_pl_df()

        variants_finished = variants_with_samples.join(
            existing_alleles.lazy(),
            on=[
                StandardColumnNames.region,
                StandardColumnNames.position_nt,
                StandardColumnNames.alt_nt
            ],
            how='left'
        ).join(
            existing_amino_subs.lazy(),
            on=[
                StandardColumnNames.gff_feature,
                StandardColumnNames.position_aa,
                StandardColumnNames.alt_aa
            ],
            how='left'
        )

        mutations_finished = mutations_with_samples.join(
            existing_alleles.lazy(),
            on=[
                StandardColumnNames.region,
                StandardColumnNames.position_nt,
                StandardColumnNames.alt_nt
            ],
            how='left'
        ).join(
            existing_amino_subs.lazy(),
            on=[
                StandardColumnNames.gff_feature,
                StandardColumnNames.position_aa,
                StandardColumnNames.alt_aa
            ],
            how='left'
        )
        return variants_finished, mutations_finished

    @staticmethod
    async def _insert_new_translations(
        variants_finished: pl.LazyFrame,
        mutations_finished: pl.LazyFrame
    ) -> str:
        # 11. Split out and insert new translations from vars and muts
        translations_cols = {
            StandardColumnNames.allele_id,
            StandardColumnNames.amino_acid_substitution_id
        }

        translations = pl.concat(
            [
                variants_finished.select(translations_cols),
                mutations_finished.select(translations_cols)
            ]
        ).drop_nulls().unique()

        # 11a. Filter to new translations and insert
        existing_translations = await get_all_translations_as_pl_df()
        new_translations = translations.join(
            existing_translations.lazy(),
            on=[
                StandardColumnNames.allele_id,
                StandardColumnNames.amino_acid_substitution_id
            ],
            how='anti'
        )
        return await copy_insert_translations(new_translations.collect())

    @staticmethod
    async def _insert_new_mutations(mutations_finished: pl.LazyFrame) -> str:
        # 12. Filter out existing mutations (updates not allowed)
        existing_mutations = await get_all_mutations_as_pl_df()
        new_mutations = mutations_finished.join(
            existing_mutations.lazy(),
            on=[
                StandardColumnNames.sample_id,
                StandardColumnNames.allele_id
            ],
            how='anti'
        )

        # 13. insert new mutations via copy
        return await copy_insert_mutations(new_mutations.collect())

    @staticmethod
    async def _update_existing_variants(variants_finished: pl.LazyFrame, existing_variants: pl.DataFrame) -> int:
        # 14. Separate new and existing variants
        updated_variants: pl.DataFrame = variants_finished.join(
            existing_variants.lazy(),
            on=[
                StandardColumnNames.sample_id,
                StandardColumnNames.allele_id
            ],
            how='inner'
        ).collect()

        count_preexisting_variants = len(updated_variants)

        # 16. Update existing variants (new bulk process for this?)
        await batch_upsert_variants(updated_variants)
        return count_preexisting_variants

    @staticmethod
    async def _insert_new_variants(variants_finished: pl.LazyFrame, existing_variants: pl.DataFrame) -> str:
        # 14. Separate new and existing variants
        new_variants = variants_finished.join(
            existing_variants.lazy(),
            on=[
                StandardColumnNames.sample_id,
                StandardColumnNames.allele_id
            ],
            how='anti'
        )

        # 15. Insert new variants via copy
        return await copy_insert_variants(new_variants.collect())

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return {
            f' variants: {", ".join(VariantsTsvParser.get_required_column_set())}',
            f'mutations: {", ".join(MutationsTsvParser.get_required_column_set())}'
        }

    def _verify_headers(self):
        with open(self.variants_filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            required_columns = VariantsTsvParser.get_required_column_set()
            if not set(reader.fieldnames) >= required_columns:
                raise ValueError(f'Missing required fields: {required_columns - set(reader.fieldnames)}')

        with open(self.mutations_filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            required_columns = MutationsTsvParser.get_required_column_set()
            if not set(reader.fieldnames) >= required_columns:
                raise ValueError(f'Missing required fields: {required_columns - set(reader.fieldnames)}')
