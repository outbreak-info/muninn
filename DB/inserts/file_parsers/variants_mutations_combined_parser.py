import csv
import time
from datetime import datetime
from math import ceil
from typing import List, Set

import dask.dataframe as dd
import polars as pl
from dask.diagnostics import ResourceProfiler

from DB.inserts.alleles import copy_insert_alleles
from DB.inserts.amino_acid_substitutions import copy_insert_aa_subs
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.mutations import copy_insert_mutations, batch_upsert_mutations
from DB.inserts.translations import copy_insert_translations
from DB.inserts.variants import batch_upsert_variants, copy_insert_variants
from DB.queries.alleles import get_all_alleles_as_pl_df, get_all_alleles_as_dask_df
from DB.queries.amino_acid_substitutions import get_all_amino_acid_subs_as_pl_df
from DB.queries.mutations import get_all_mutations_as_pl_df
from DB.queries.samples import get_samples_accession_and_id_as_pl_df, get_samples_accession_and_id_as_dask_df
from DB.queries.translations import get_all_translations_as_pl_df
from DB.queries.variants import get_all_variants_as_pl_df
from utils.constants import StandardColumnNames, EXCLUDED_SRAS

AMINO_SUB_REF_CONFLICTS_FILE = '/tmp/amino_sub_ref_conflicts.csv'
ALLELE_REF_CONFLICTS_FILE = '/tmp/allele_ref_conflicts.csv'
TRANSLATIONS_REF_CONFLICTS_FILE = '/tmp/translations_ref_conflicts.csv'


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
        # todo: add some timestamps to debug data.
        debug_info = {
            'count_alleles_added': 'unset',
            'count_amino_subs_added': 'unset',
            'count_translations_added': 'unset',
            'count_mutations_added': 'unset',
            'count_preexisting_variants': 'unset',
            'count_variants_added': 'unset',
            'count_preexisting_mutations': 'unset',
        }
        t0 = time.time()
        #  1. read vars and muts
        variants: pl.LazyFrame = await self._scan_variants()
        mutations: pl.LazyFrame = await self._scan_mutations()

        #  2. Get accession -> id mapping from db
        #  3. Filter out vars and muts with accessions missing from db
        variants, mutations = await (
            VariantsMutationsCombinedParser._get_vars_and_muts_for_existing_samples(variants, mutations)
        )
        t1 = time.time()
        print(f'read files, filtered for existing samples. Elapsed: {t1 - t0}')
        #  4. split out and combine alleles
        #  5. filter out existing alleles
        #  6. Insert new alleles via copy
        debug_info['count_alleles_added'] = await VariantsMutationsCombinedParser._insert_new_alleles(
            variants,
            mutations,
        )
        print(f'alleles added: {debug_info}')
        t2 = time.time()
        print(f'inserted new alleles. Elapsed: {t2 - t1}')

        #  7. split out and combine amino acid subs
        #  8. filter out existing AA subs
        #  9. insert new aa subs via copy
        debug_info['count_amino_subs_added'] = await VariantsMutationsCombinedParser._insert_new_amino_acids(
            variants,
            mutations
        )
        print(f'amino subs added: {debug_info}')
        t3 = time.time()
        print(f'inserted new amino acids. Elapsed: {t3 - t2}')

        # 10. Get new allele / AAS ids and join back into vars and muts
        # vars and muts now need to include translation ids before they are finished
        variants, mutations = await (
            VariantsMutationsCombinedParser
            ._join_alleles_and_amino_subs_into_vars_and_muts(variants, mutations)
        )

        # 11. Split out and insert new translations from vars and muts
        debug_info['count_translations_added'] = await (
            VariantsMutationsCombinedParser
            ._insert_new_translations(variants, mutations)
        )
        print(f'translations added: {debug_info}')
        t4 = time.time()
        print(f'inserted translations. Elapsed: {t4 - t3}')

        # 11.5: Combine translation ids back into vars and muts
        variants, mutations = await (
            VariantsMutationsCombinedParser._join_translation_ids_into_vars_and_muts(
                variants,
                mutations
            )
        )

        count_variants_added, count_preexisting_variants = await(
            VariantsMutationsCombinedParser._insert_and_update_variants(variants)
        )
        debug_info['count_variants_added'] = count_variants_added
        debug_info['count_preexisting_variants'] = count_preexisting_variants
        print(f'variants added / updated: {debug_info}')

        count_mutations_added, count_preexisting_mutations = await(
            VariantsMutationsCombinedParser._insert_and_update_mutations(mutations)
        )
        debug_info['count_mutations_added'] = count_mutations_added
        debug_info['count_preexisting_mutations'] = count_preexisting_mutations
        print(f'mutations added / updated: {debug_info}')

    async def _scan_variants(self):
        def variants_colname_mapping(cns: List[str]) -> List[str]:
            mapped_names = []
            inverted_colname_map = {v: k for k, v in VariantsMutationsCombinedParser.variants_column_mapping.items()}
            for cn in cns:
                try:
                    mapped_names.append(inverted_colname_map[cn])
                except KeyError:
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
            inverted_colname_map = {v: k for k, v in VariantsMutationsCombinedParser.mutations_column_mapping.items()}
            for cn in cns:
                try:
                    mapped_names.append(inverted_colname_map[cn])
                except KeyError:
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
                # clean up gff feature (HA:cds-XAJ25415.1  -->  XAJ25415.1)
                .str.extract(r'^\s*([\w\-]+:)?(cds-)?(.*)\s*$', 3)
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
        ).filter(  # remove problematic/redacted SRAs
            ~pl.col(StandardColumnNames.accession).is_in(EXCLUDED_SRAS) &
            # filter out deletions with no NT data, formatted as +N
            pl.col(StandardColumnNames.alt_nt).str.count_matches(r'\+\d+') == 0

        )

    @staticmethod
    async def _get_vars_and_muts_for_existing_samples(
        variants_input: pl.LazyFrame,
        mutations_input: pl.LazyFrame,
        existing_samples: pl.DataFrame | None = None
    ) -> (pl.LazyFrame, pl.LazyFrame):
        if existing_samples is None:
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
            print(
                f'WARNING: in alleles, found {len(ref_conflicts)} positions with conflicting values for ref_nt. '
                f'Written to {ALLELE_REF_CONFLICTS_FILE}'
            )
            ref_conflicts.write_csv(ALLELE_REF_CONFLICTS_FILE)

        # ref conflicts have already been filtered out of new_alleles above, so we are good to insert
        #  6. Insert new alleles via copy
        return await copy_insert_alleles(new_alleles.collect())

    @staticmethod
    async def _insert_new_amino_acids(
        variants_with_samples: pl.LazyFrame,
        mutations_with_samples: pl.LazyFrame,
    ) -> str:
        #  7. split out and combine amino acid subs
        amino_sub_cols = {
            StandardColumnNames.gff_feature,
            StandardColumnNames.position_aa,
            StandardColumnNames.ref_aa,
            StandardColumnNames.alt_aa
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
            print(
                f'WARNING: in amino acid subs, found {len(ref_conflicts)} positions with conflicting values for ref_aa. '
                f'Written to {AMINO_SUB_REF_CONFLICTS_FILE}'
            )
            ref_conflicts.write_csv(AMINO_SUB_REF_CONFLICTS_FILE)

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

        variants_with_nt_aa_ids = variants_with_samples.join(
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

        mutations_with_nt_aa_ids = mutations_with_samples.join(
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

        return variants_with_nt_aa_ids, mutations_with_nt_aa_ids

    @staticmethod
    async def _insert_new_translations(
        variants_finished: pl.LazyFrame,
        mutations_finished: pl.LazyFrame
    ) -> str:
        # 11. Split out and insert new translations from vars and muts
        translations_cols = {
            StandardColumnNames.ref_codon,
            StandardColumnNames.alt_codon,
            StandardColumnNames.amino_acid_id
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
                StandardColumnNames.alt_codon,
                StandardColumnNames.amino_acid_id
            ],
            how='anti'
        ).unique(
            [
                pl.col(StandardColumnNames.alt_codon),
                pl.col(StandardColumnNames.amino_acid_id)
            ]
        )

        # check for conflicts on ref codon
        ref_conflicts = (
            pl.concat(
                [
                    existing_translations.select(
                        {
                            StandardColumnNames.ref_codon,
                            StandardColumnNames.alt_codon,
                            StandardColumnNames.amino_acid_id
                        }
                    ).lazy(),
                    translations
                ]
            )
            .unique()
            .group_by(
                pl.col(StandardColumnNames.amino_acid_id),
                pl.col(StandardColumnNames.alt_codon)
            )
            .len()
            .filter(pl.col('len') > 1)
            .select(
                pl.col(StandardColumnNames.alt_codon, StandardColumnNames.amino_acid_id)
            )
        ).collect()
        if len(ref_conflicts) > 0:
            print(
                f'WARNING: in translations, found {len(ref_conflicts)} positions with conflicting values for ref_codon. '
                f'Written to {TRANSLATIONS_REF_CONFLICTS_FILE}'
            )
            ref_conflicts.write_csv(TRANSLATIONS_REF_CONFLICTS_FILE)

        return await copy_insert_translations(new_translations.collect())

    @staticmethod
    async def _join_translation_ids_into_vars_and_muts(
        variants_with_nt_aa_ids: pl.LazyFrame,
        mutations_with_nt_aa_ids: pl.LazyFrame,
    ) -> (pl.LazyFrame, pl.LazyFrame):
        # 11.5: Combine translation ids back into vars and muts
        existing_translations = await get_all_translations_as_pl_df()

        variants_plus_translation_ids = variants_with_nt_aa_ids.join(
            existing_translations.lazy(),
            on=[
                StandardColumnNames.amino_acid_id,
                StandardColumnNames.alt_codon
            ],
            how='left'
        )

        mutations_plus_translation_ids = mutations_with_nt_aa_ids.join(
            existing_translations.lazy(),
            on=[
                StandardColumnNames.amino_acid_id,
                StandardColumnNames.alt_codon
            ],
            how='left'
        )
        return variants_plus_translation_ids, mutations_plus_translation_ids

    @staticmethod
    async def _insert_and_update_mutations(mutations: pl.LazyFrame) -> (str, int):
        # mutations_collected, profile_mutations = mutations.profile(engine='streaming')
        # profile_mutations = profile_mutations.with_columns(elapsed=pl.col('end') - pl.col('start'))
        # print(profile_mutations)
        # t7 = time.time()
        # print(f'collected mutations. Elapsed: {t7 - t6}')

        # 12. Separate new and existing mutations.
        # 12.5: Update existing mutations
        # 13. insert new mutations via copy
        existing_mutations: pl.DataFrame = await get_all_mutations_as_pl_df()

        # what cols are necessary in mutations?
        # just sample_id, allele_id, translation_id
        mutations = mutations.select(
            [
                pl.col(StandardColumnNames.sample_id),
                pl.col(StandardColumnNames.allele_id),
                pl.col(StandardColumnNames.translation_id)
            ]
        )

        count_mutations_added = await (
            VariantsMutationsCombinedParser._insert_new_mutations(mutations, existing_mutations)
        )
        count_preexisting_mutations = await (
            VariantsMutationsCombinedParser._update_existing_mutations(mutations, existing_mutations)
        )
        # rm
        # t8 = time.time()
        # print(f'added / updated mutations. Elapsed: {t8 - t7}')
        return count_mutations_added, count_preexisting_mutations

    @staticmethod
    async def _insert_new_mutations(mutations: pl.LazyFrame, existing_mutations: pl.DataFrame) -> str:
        # 12. Separate new and existing mutations.
        t0 = time.time()
        new_mutations: pl.DataFrame = mutations.join(
            existing_mutations.lazy(),
            on=[
                StandardColumnNames.sample_id,
                StandardColumnNames.allele_id
            ],
            how='anti'
        ).collect(engine='streaming')
        t1 = time.time()
        print(f'Collected new mutations in: {t1 - t0}')
        # 13. insert new mutations via copy
        return await copy_insert_mutations(new_mutations)

    @staticmethod
    async def _update_existing_mutations(mutations: pl.LazyFrame, existing_mutations: pl.DataFrame) -> int:
        # 12. Separate new and existing mutations.
        # 12.5: Update existing mutations
        t0 = time.time()
        updated_mutations: pl.DataFrame = mutations.join(
            existing_mutations.lazy(),
            on=[
                StandardColumnNames.sample_id,
                StandardColumnNames.allele_id
            ],
            how='inner'
        ).collect(engine='streaming')
        count_preexisting_mutations = len(updated_mutations)
        t1 = time.time()
        print(f'Collected existing mutations in: {t1 - t0}')
        await batch_upsert_mutations(updated_mutations)
        return count_preexisting_mutations

    @staticmethod
    async def _insert_and_update_variants(variants: pl.LazyFrame) -> (str, int):
        # variants_collected, profile_variants = variants.profile(engine='streaming')
        # profile_variants = profile_variants.with_columns(elapsed=pl.col('end') - pl.col('start'))
        # print(profile_variants)
        # t5 = time.time()
        # print(f'collected variants. Elapsed: {t5 - t4}')

        # 14. Separate new and existing variants
        # 15. Insert new variants via copy
        # 16. Update existing variants (new bulk process for this?)

        existing_variants = await get_all_variants_as_pl_df()

        variants: pl.LazyFrame = variants.join(
            existing_variants.lazy(),
            on=[
                StandardColumnNames.sample_id,
                StandardColumnNames.allele_id
            ],
            how='left'
        )

        count_variants_added = await (
            VariantsMutationsCombinedParser._insert_new_variants(variants)
        )
        count_preexisting_variants = await (
            VariantsMutationsCombinedParser._update_existing_variants(variants)
        )

        # t6 = time.time()
        # print(f'added / updated variants. Elapsed: {t6 - t5}')
        return count_variants_added, count_preexisting_variants

    @staticmethod
    async def _update_existing_variants(variants: pl.LazyFrame) -> int:
        # 14. Separate new and existing variants
        t0 = time.time()
        updated_variants: pl.DataFrame = variants.filter(
            pl.col(StandardColumnNames.intra_host_variant_id).is_not_null()
        ).collect(engine='streaming')
        t1 = time.time()
        print(f'Collected existing variants in: {t1 - t0}')
        count_preexisting_variants = len(updated_variants)

        # 16. Update existing variants (new bulk process for this?)
        await batch_upsert_variants(updated_variants)
        return count_preexisting_variants

    @staticmethod
    async def _insert_new_variants(variants: pl.LazyFrame) -> str:
        # 14. Separate new and existing variants
        t0 = time.time()
        new_variants = variants.filter(
            pl.col(StandardColumnNames.intra_host_variant_id).is_null()
        ).collect(engine='streaming')
        t1 = time.time()
        print(f'Collected new variants in: {t1 - t0}')
        # 15. Insert new variants via copy
        return await copy_insert_variants(new_variants)

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return {
            f' variants: {", ".join(VariantsMutationsCombinedParser.variants_column_mapping.values())}',
            f'mutations: {", ".join(VariantsMutationsCombinedParser.mutations_column_mapping.values())}'
        }

    def _verify_headers(self):
        with open(self.variants_filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            required_columns = set(VariantsMutationsCombinedParser.variants_column_mapping.values())
            if not set(reader.fieldnames) >= required_columns:
                raise ValueError(f'Missing required fields: {required_columns - set(reader.fieldnames)}')

        with open(self.mutations_filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            required_columns = set(VariantsMutationsCombinedParser.mutations_column_mapping.values())
            if not set(reader.fieldnames) >= required_columns:
                raise ValueError(f'Missing required fields: {required_columns - set(reader.fieldnames)}')

    variants_column_mapping = {
        StandardColumnNames.region: 'REGION',
        StandardColumnNames.position_nt: 'POS',
        StandardColumnNames.ref_nt: 'REF',
        StandardColumnNames.alt_nt: 'ALT',
        StandardColumnNames.position_aa: 'POS_AA',
        StandardColumnNames.ref_aa: 'REF_AA',
        StandardColumnNames.alt_aa: 'ALT_AA',
        StandardColumnNames.gff_feature: 'GFF_FEATURE',
        StandardColumnNames.ref_codon: 'REF_CODON',
        StandardColumnNames.alt_codon: 'ALT_CODON',
        StandardColumnNames.accession: 'SRA',
        StandardColumnNames.pval: 'PVAL',
        StandardColumnNames.ref_dp: 'REF_DP',
        StandardColumnNames.ref_rv: 'REF_RV',
        StandardColumnNames.ref_qual: 'REF_QUAL',
        StandardColumnNames.alt_dp: 'ALT_DP',
        StandardColumnNames.alt_rv: 'ALT_RV',
        StandardColumnNames.alt_qual: 'ALT_QUAL',
        StandardColumnNames.pass_qc: 'PASS',
        StandardColumnNames.alt_freq: 'ALT_FREQ',
        StandardColumnNames.total_dp: 'TOTAL_DP',
    }

    mutations_column_mapping = {
        StandardColumnNames.accession: 'sra',
        StandardColumnNames.position_nt: 'pos',
        StandardColumnNames.ref_nt: 'ref',
        StandardColumnNames.alt_nt: 'alt',
        StandardColumnNames.region: 'region',
        StandardColumnNames.gff_feature: 'GFF_FEATURE',
        StandardColumnNames.ref_codon: 'ref_codon',
        StandardColumnNames.alt_codon: 'alt_codon',
        StandardColumnNames.ref_aa: 'ref_aa',
        StandardColumnNames.alt_aa: 'alt_aa',
        StandardColumnNames.position_aa: 'pos_aa',
    }


class VariantsMutationsCombinedChunkedParser(VariantsMutationsCombinedParser):
    CHUNK_SIZE = 50_000

    async def parse_and_insert(self):
        debug_info = {
            'alleles_added': 'unset',
            'timestamp_alleles_added': 'unset',
            'amino_acids_added': 'unset',
            'timestamp_amino_acids_added': 'unset',
            'translations_added': 'unset',
            'timestamp_translations_added': 'unset',
            'variant_chunk_timestamps': [],
            'mutation_chunk_timestamps': [],
            'variant_chunk_sizes': [],
            'mutation_chunk_sizes': []
        }
        # 1) read and cleanup the variants and mutations as before
        variants: pl.LazyFrame = await self._scan_variants()
        mutations: pl.LazyFrame = await self._scan_mutations()

        # 2) Get all the samples + accessions out of the DB
        existing_samples: pl.DataFrame = await get_samples_accession_and_id_as_pl_df()

        # 2.1) Filter to existing samples as before
        variants, mutations = await VariantsMutationsCombinedParser._get_vars_and_muts_for_existing_samples(
            variants,
            mutations,
            existing_samples
        )

        # 3) Do alleles as before
        debug_info['alleles_added'] = await VariantsMutationsCombinedParser._insert_new_alleles(
            variants,
            mutations
        )
        debug_info['timestamp_alleles_added'] = datetime.now().isoformat()
        print(f'alleles added: {debug_info}')

        # 4) do aminos as before
        debug_info['amino_acids_added'] = await VariantsMutationsCombinedParser._insert_new_amino_acids(
            variants,
            mutations
        )
        debug_info['timestamp_amino_acids_added'] = datetime.now().isoformat()
        print(f'amino acids added: {debug_info}')

        # 5) Lazy-join alleles and amino acids back into vars and muts
        variants, mutations = await (
            VariantsMutationsCombinedParser
            ._join_alleles_and_amino_subs_into_vars_and_muts(variants, mutations)
        )

        # 6) do translations as before
        debug_info['translations_added'] = await VariantsMutationsCombinedParser._insert_new_translations(
            variants,
            mutations
        )
        debug_info['timestamp_translations_added'] = datetime.now().isoformat()
        print(f'translations added: {debug_info}')

        # 7) Lazy-join translations back into the variants and mutations frames
        variants, mutations = await(
            VariantsMutationsCombinedParser._join_translation_ids_into_vars_and_muts(variants, mutations)
        )

        # 8) Iterate through sample accessions in chunks, and process connected variants and mutations.
        # 8.1) Get chunk of samples, and for each chunk...
        n_samples = existing_samples.select(pl.len()).item()
        print(
            f'variants and mutations will be processed in '
            f'{ceil(n_samples / VariantsMutationsCombinedChunkedParser.CHUNK_SIZE)} chunks'
        )
        chunk_start = 0
        while chunk_start < n_samples:
            chunk_sample_ids = (
                existing_samples
                .slice(chunk_start, VariantsMutationsCombinedChunkedParser.CHUNK_SIZE)
                .select(pl.col(StandardColumnNames.sample_id))
            )
            chunk_start += VariantsMutationsCombinedChunkedParser.CHUNK_SIZE

            # 8.2) Filter to connected variants
            # 8.3) Upsert variants
            debug_info['variant_chunk_sizes'].append(
                await VariantsMutationsCombinedChunkedParser._upsert_variants_chunk(variants, chunk_sample_ids)
            )
            debug_info['variant_chunk_timestamps'].append(datetime.now().isoformat())

            # 8.4) Filter to connected mutations
            # 8.5) upsert mutations
            debug_info['mutation_chunk_sizes'].append(
                await VariantsMutationsCombinedChunkedParser._upsert_mutations_chunk(mutations, chunk_sample_ids)
            )
            debug_info['mutation_chunk_timestamps'].append(datetime.now().isoformat())
            print(f'Chunk finished: {debug_info}')

    @staticmethod
    async def _upsert_variants_chunk(variants: pl.LazyFrame, chunk_sample_ids: pl.DataFrame) -> int:
        # 8.2) Filter to connected variants
        chunk_variants: pl.DataFrame = variants.join(
            chunk_sample_ids.lazy(),
            on=pl.col(StandardColumnNames.sample_id),
            how='inner'
        ).collect(engine='streaming')

        # 8.3) Upsert variants
        await batch_upsert_variants(chunk_variants)
        return chunk_variants.select(pl.len()).item()

    @staticmethod
    async def _upsert_mutations_chunk(mutations: pl.LazyFrame, chunk_sample_ids: pl.DataFrame) -> int:
        # 8.4) Filter to connected mutations
        chunk_mutations = (
            mutations.join(
                chunk_sample_ids.lazy(),
                on=pl.col(StandardColumnNames.sample_id),
                how='inner'
            )
        ).collect(engine='streaming')
        chunk_mutations.write_csv('/home/muninn/data/last_mutations_chunk.csv')  # rm
        # 8.5) Upsert mutations
        await batch_upsert_mutations(chunk_mutations)
        return chunk_mutations.select(pl.len()).item()


class VariantsMutationsCombinedParserDask(VariantsMutationsCombinedParser):

    def __init__(self, variants_filename: str, mutations_filename: str):
        super().__init__(variants_filename, mutations_filename)
        # todo: make configurable
        self.blocksize = '128MB'

    async def parse_and_insert(self):
        # rm
        rprof = ResourceProfiler()
        rprof.register()

        existing_samples: dd.DataFrame = await get_samples_accession_and_id_as_dask_df()

        mutations: dd.DataFrame = await self.read_mutations_file()
        mutations = VariantsMutationsCombinedParserDask._clean_up_gff_feature(mutations)

        variants: dd.DataFrame = await self.read_variants_file()
        variants = VariantsMutationsCombinedParserDask._clean_up_gff_feature(variants)

        # join in sample ids, filter down to existing samples
        # todo: drop the accessions at this point?
        mutations = mutations.join(
            existing_samples,
            on=StandardColumnNames.accession,
            how='inner'
        )
        variants = variants.join(
            existing_samples,
            on=StandardColumnNames.accession,
            how='inner'
        )

        # alleles
        combined_alleles: dd.DataFrame = (
            dd.concat(
                [
                    mutations[[
                        StandardColumnNames.region,
                        StandardColumnNames.ref_nt,
                        StandardColumnNames.position_nt,
                        StandardColumnNames.alt_nt
                    ]],
                    variants[[
                        StandardColumnNames.region,
                        StandardColumnNames.ref_nt,
                        StandardColumnNames.position_nt,
                        StandardColumnNames.alt_nt
                    ]]
                ]
            ).drop_duplicates(ignore_index=True)
        )

        # find ref conflicts
        ref_conflicts: dd.DataFrame = (
            combined_alleles.groupby(
                by=[
                    StandardColumnNames.region,
                    StandardColumnNames.position_nt,
                    StandardColumnNames.alt_nt
                ]
            )
            .count()
            .query(f'{StandardColumnNames.ref_nt} > 1')
            .compute()
        )
        # todo: warn about ref conflicts

        combined_alleles = combined_alleles.drop_duplicates(
            subset=[
                StandardColumnNames.region,
                StandardColumnNames.position_nt,
                StandardColumnNames.alt_nt
            ],
            ignore_index=True
        )

        # get new alleles
        existing_alleles: dd.DataFrame = await get_all_alleles_as_dask_df()
        new_alleles: dd.DataFrame = dd.DataFrame.from_dict({})
        if len(existing_alleles.index) == 0:
            new_alleles = combined_alleles
        else:
            alleles_with_ids: dd.DataFrame = combined_alleles.merge(
                existing_alleles,
                on=[
                    StandardColumnNames.region,
                    StandardColumnNames.position_nt,
                    StandardColumnNames.alt_nt
                ],
                how='left'
            )

            updated_alleles: dd.DataFrame = alleles_with_ids.dropna(subset=[StandardColumnNames.allele_id])
            # todo: update alleles
            # todo: calculate new alleles too
        # insert new alleles
        status = await copy_insert_alleles(new_alleles)
        print(status)

        # get alleles again
        existing_alleles: dd.DataFrame = await get_all_alleles_as_dask_df()

        # join to mutations and variants
        # keep just the ref from the db after this
        mutations = (
            mutations.merge(
                existing_alleles,
                on=[
                    StandardColumnNames.region,
                    StandardColumnNames.position_nt,
                    StandardColumnNames.alt_nt
                ],
                how='left'
            )
            .drop(columns=f'{StandardColumnNames.ref_nt}_x')
            .rename(columns={f'{StandardColumnNames.ref_nt}_y': StandardColumnNames.ref_nt})
        )
        # amino acids
        # translations
        # mutations
        # variants


        # rm
        with open(f'data/mutations.rprof.{datetime.now().isoformat(timespec="seconds")}.csv', 'w+') as f:
            writer = csv.writer(f)
            writer.writerow(['time', 'mem', 'cpu'])
            writer.writerows(rprof.results)
        # rm
        print(mutations.info())
        print(mutations.memory_usage())
        return

    async def read_mutations_file(self):
        dtype_mapping = {
            v: VariantsMutationsCombinedParserDask.mutations_column_types[k]
            for k, v in VariantsMutationsCombinedParser.mutations_column_mapping.items()
        }

        mutations: dd.DataFrame = dd.read_csv(
            self.mutations_filename,
            delimiter=self.delimiter,
            dtype=dtype_mapping,
            blocksize=self.blocksize
        )
        rename_mapping = {v: k for k, v in VariantsMutationsCombinedParser.mutations_column_mapping.items()}
        mutations = mutations.rename(columns=rename_mapping)

        return mutations

    async def read_variants_file(self):
        dtype_mapping = {
            v: VariantsMutationsCombinedParserDask.variants_column_types[k]
            for k, v in VariantsMutationsCombinedParser.variants_column_mapping.items()
        }
        variants: dd.DataFrame = dd.read_csv(
            self.variants_filename,
            delimiter=self.delimiter,
            dtype=dtype_mapping,
            blocksize=self.blocksize
        )
        rename_mapping = {v: k for k, v in VariantsMutationsCombinedParser.variants_column_mapping.items()}
        variants = variants.rename(columns=rename_mapping)
        return variants

    @staticmethod
    def _clean_up_gff_feature(df: dd.DataFrame) -> dd.DataFrame:
        # todo: this is a bug waiting to happen, GFF features should be cleaned up before we see them here.
        # clean up gff feature (HA:cds-XAJ25415.1  -->  XAJ25415.1)
        return df.replace(
            {StandardColumnNames.gff_feature: r'^\s*([\w\-]+:)?(cds-)?'},
            {StandardColumnNames.gff_feature: ''},
            regex=True
        )

    def _verify_headers(self):
        super()._verify_headers()

    # todo: try reading in the region as a categorical
    mutations_column_types = {
        StandardColumnNames.accession: 'string',
        StandardColumnNames.position_nt: 'Int32',
        StandardColumnNames.ref_nt: 'string',
        StandardColumnNames.alt_nt: 'string',
        StandardColumnNames.region: 'string',
        StandardColumnNames.gff_feature: 'string',
        StandardColumnNames.ref_codon: 'string',
        StandardColumnNames.alt_codon: 'string',
        StandardColumnNames.ref_aa: 'string',
        StandardColumnNames.alt_aa: 'string',
        StandardColumnNames.position_aa: 'Int32',
    }

    variants_column_types = {
        StandardColumnNames.region: 'string',
        StandardColumnNames.position_nt: 'Int32',
        StandardColumnNames.ref_nt: 'string',
        StandardColumnNames.alt_nt: 'string',
        StandardColumnNames.position_aa: 'Int32',
        StandardColumnNames.ref_aa: 'string',
        StandardColumnNames.alt_aa: 'string',
        StandardColumnNames.gff_feature: 'string',
        StandardColumnNames.ref_codon: 'string',
        StandardColumnNames.alt_codon: 'string',
        StandardColumnNames.accession: 'string',
        StandardColumnNames.pval: 'Float32',
        StandardColumnNames.ref_dp: 'Int32',
        StandardColumnNames.ref_rv: 'Int32',
        StandardColumnNames.ref_qual: 'Float32',
        StandardColumnNames.alt_dp: 'Int32',
        StandardColumnNames.alt_rv: 'Int32',
        StandardColumnNames.alt_qual: 'Float32',
        StandardColumnNames.pass_qc: 'boolean',
        StandardColumnNames.alt_freq: 'Float32',
        StandardColumnNames.total_dp: 'Int32',
    }
