from collections import defaultdict
from typing import List, Dict

from sqlalchemy import text

from DB.engine import get_async_session
from api.models import VariantMutationLagInfo
from utils.constants import TableNames, ColumnNames


async def get_mutations_before_variants(
    lineage: str,
    lineage_system_name: str
) -> Dict[str, List[VariantMutationLagInfo]]:
    return await _get_lag_variants_mutations(
        lineage,
        lineage_system_name,
        'fm.start_date < fv.start_date',
        'fv.start_date::date - fm.start_date::date'
    )


async def get_variants_before_mutations(
    lineage: str,
    lineage_system_name: str
) -> Dict[str, List[VariantMutationLagInfo]]:
    return await _get_lag_variants_mutations(
        lineage,
        lineage_system_name,
        'fv.start_date < fm.start_date',
        'fm.start_date::date - fv.start_date::date'
    )


async def _get_lag_variants_mutations(
    lineage: str,
    lineage_system_name: str,
    lag_condition: str,
    lag_calculation: str
) -> Dict[str, List[VariantMutationLagInfo]]:
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                WITH sample_subset AS (
                    SELECT s.id, s.collection_start_date from samples s
                    INNER JOIN samples_lineages sl ON s.id = sl.sample_id
                    INNER JOIN lineages l ON sl.lineage_id = l.id
                    INNER JOIN lineage_systems ls ON l.lineage_system_id = ls.id
                    WHERE l.lineage_name = :lineage AND ls.lineage_system_name = :lineage_system_name AND collection_end_date - collection_start_date <= 30
                ),
                sample_subset_bm AS (
                    SELECT rb_build_agg(id) AS bm FROM sample_subset
                ),
                first_mutations AS (
                    SELECT MIN(ss.collection_start_date) as start_date, aa.ref_aa, aa.position_aa, aa.alt_aa, aa.gff_feature
                    FROM sample_subset ss
                    INNER JOIN {TableNames.cns_amino_acids_by_sample} caas ON caas.{ColumnNames.sample_id} = ss.id
                    CROSS JOIN LATERAL unnest(rb_to_array(caas.{ColumnNames.amino_acids_present})) AS u({ColumnNames.amino_acid_id})
                    INNER JOIN {TableNames.amino_acids} aa ON aa.id = u.{ColumnNames.amino_acid_id}
                    GROUP BY aa.ref_aa, aa.position_aa, aa.alt_aa, aa.gff_feature
                ),
                candidate_aa AS (
                    SELECT DISTINCT aa.id
                    FROM {TableNames.amino_acids} aa
                    INNER JOIN first_mutations fm
                        ON fm.ref_aa = aa.ref_aa
                        AND fm.position_aa = aa.position_aa
                        AND fm.alt_aa = aa.alt_aa
                        AND fm.gff_feature = aa.gff_feature
                ),
                first_variants AS (
                    SELECT MIN(ss.collection_start_date) as start_date, aa.ref_aa, aa.position_aa, aa.alt_aa, aa.gff_feature
                    FROM candidate_aa c
                    INNER JOIN {TableNames.ih_samples_by_amino_acid} isaa
                        ON isaa.{ColumnNames.amino_acid_id} = c.id
                    CROSS JOIN LATERAL unnest(
                        rb_to_array(isaa.{ColumnNames.samples_present} & (SELECT bm FROM sample_subset_bm))
                    ) AS u({ColumnNames.sample_id})
                    INNER JOIN sample_subset ss ON ss.id = u.{ColumnNames.sample_id}
                    INNER JOIN {TableNames.amino_acids} aa ON aa.id = isaa.{ColumnNames.amino_acid_id}
                    GROUP BY aa.ref_aa, aa.position_aa, aa.alt_aa, aa.gff_feature
                )
                SELECT fv.start_date as variants_start_date, fm.start_date as mutations_start_date, ({lag_calculation}) as lag, fv.ref_aa, fv.position_aa, fv.alt_aa, fv.gff_feature from
                    first_variants fv
                    INNER JOIN first_mutations fm ON fv.ref_aa = fm.ref_aa AND fv.position_aa = fm.position_aa AND fv.alt_aa = fm.alt_aa AND fv.gff_feature = fm.gff_feature
                    WHERE {lag_condition};
                    '''
            ),
            {
                'lineage': lineage,
                'lineage_system_name': lineage_system_name
            }
        )

    out = defaultdict(list)
    for r in res:
        gff_feature = r[6]
        out[gff_feature].append(
            VariantMutationLagInfo(
                variants_start_date=r[0],
                mutations_start_date=r[1],
                lag=r[2],
                ref=r[3],
                pos=r[4],
                alt=r[5]
            )
        )
    return out
