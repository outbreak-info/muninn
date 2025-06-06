from typing import List, Set

from DB.inserts.file_parsers.file_parser import FileParser
from DB.queries.samples import get_samples_accession_and_id_as_pl_df
import polars as pl

from utils.constants import StandardColumnNames
from utils.csv_helpers import gff_feature_strip_region_name
from . import variants_tsv_parser
from . import mutations_parser
from ..alleles import bulk_insert_alleles
from ..amino_acid_substitutions import bulk_insert_aa_subs
from ..mutations import bulk_insert_mutations
from ..translations import bulk_insert_translations
from ..variants import batch_upsert_variants
from ...queries.alleles import get_all_alleles_as_pl_df
from ...queries.amino_acid_substitutions import get_all_amino_acid_subs_as_pl_df
from ...queries.mutations import get_all_mutations_as_pl_df
from ...queries.translations import get_all_translations_as_pl_df
from ...queries.variants import get_all_variants_as_pl_df


class VariantsMutationsCombinedParser(FileParser):

    def __init__(self, variants_filename: str, mutations_filename: str):
        self.variants_filename = variants_filename
        self.mutations_filename = mutations_filename

    async def parse_and_insert(self):
        debug_info = {
            'count_alleles_added': 'unset',
            'count_amino_subs_added': 'unset',
            'count_translations_added': 'unset',
            'count_mutations_added': 'unset',
        }

        #  1. read vars and muts
        def variants_colname_mapping(cns: List[str]) -> List[str]:
            mapped_names = []
            for cn in cns:
                try:
                    mapped_names.append(variants_tsv_parser.ColNameMapping(cn).name)
                except ValueError:
                    mapped_names.append(cn)
            return mapped_names

        variants_input: pl.LazyFrame = pl.scan_csv(
            self.variants_filename,
            with_column_names=variants_colname_mapping,
            separator='\t'
        ).with_columns(
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
                pl.col(StandardColumnNames.accession),
                pl.col(StandardColumnNames.region),
                pl.col(StandardColumnNames.position_nt),
                pl.col(StandardColumnNames.alt_nt)
            ]
        )

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
            separator='\t'
        ).with_columns(
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
                pl.col(StandardColumnNames.accession),
                pl.col(StandardColumnNames.region),
                pl.col(StandardColumnNames.position_nt),
                pl.col(StandardColumnNames.alt_nt)
            ]
        )
        # todo: verify headers

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
            {
                StandardColumnNames.region,
                StandardColumnNames.position_nt,
                StandardColumnNames.alt_nt
            }
        )

        #  5. filter out existing alleles
        existing_alleles = await get_all_alleles_as_pl_df()

        new_alleles = alleles.join(
            existing_alleles.lazy(),
            on=[
                StandardColumnNames.region,
                StandardColumnNames.position_nt,
                StandardColumnNames.alt_nt
            ],
            how='anti'
        )

        #  6. Insert new alleles via copy
        debug_info['count_alleles_added'] = await bulk_insert_alleles(new_alleles.collect())

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
                StandardColumnNames.alt_aa
            }
        )

        #  8. filter out existing AA subs
        existing_amino_subs = await get_all_amino_acid_subs_as_pl_df()
        new_amino_subs = amino_subs.join(
            existing_amino_subs.lazy(),
            on=[
                StandardColumnNames.gff_feature,
                StandardColumnNames.position_aa,
                StandardColumnNames.alt_aa
            ],
            how='anti'
        )

        #  9. insert new aa subs via copy
        debug_info['count_amino_subs_added'] = await bulk_insert_aa_subs(new_amino_subs.collect())

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
        debug_info['count_translations_added'] = await bulk_insert_translations(new_translations.collect())

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
        debug_info['count_mutations_added'] = await bulk_insert_mutations(new_mutations.collect())
        print(debug_info)  # rm

        # 14. Separate new and existing variants

        # existing_variants = await get_all_variants_as_pl_df()
        #
        # new_variants = variants_finished.join(
        #     existing_variants.lazy(),
        #     on=[
        #         StandardColumnNames.sample_id,
        #         StandardColumnNames.allele_id
        #     ],
        #     how='anti'
        # )
        #
        # updated_variants = variants_finished.join(
        #     existing_variants.lazy(),
        #     on=[
        #         StandardColumnNames.sample_id,
        #         StandardColumnNames.allele_id
        #     ],
        #     how='inner'
        # )

        # 15. Insert new variants via copy
        # 16. Update existing variants (new bulk process for this?)
        # we're going to try just doing this all at once

        await batch_upsert_variants(variants_finished.collect())

        print('done')  # rm

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        pass
