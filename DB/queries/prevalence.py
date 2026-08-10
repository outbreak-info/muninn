from typing import List

from sqlalchemy import text

from DB.engine import get_async_session
from DB.queries.helpers import get_ih_table_and_change_cols
from api.models import VariantFreqInfo, VariantCountPhenoScoreInfo, MutationCountInfo
from parser.parser import parser
from utils.constants import ColumnNames, NtOrAa, TableNames
from utils.csv_helpers import parse_change_string


async def get_samples_variant_freq_by_aa_change(change: str) -> List[VariantFreqInfo]:
    return await _get_samples_variant_freq(change, NtOrAa.aa)


async def get_samples_variant_freq_by_nt_change(change: str) -> List[VariantFreqInfo]:
    return await _get_samples_variant_freq(change, NtOrAa.nt)


async def _get_samples_variant_freq(change: str, change_bin: NtOrAa) -> List[VariantFreqInfo]:
    """
    Per-sample intra-host frequency for one change. Exact alt_freq no longer exists, so the frequency
    of a sample is the bin its observation was filed under; a sample appears once per bin it was seen
    in, which in practice is exactly once.
    """
    feature, ref, position, alt = parse_change_string(change)
    ih_table, change_id_col, catalog_table, feature_col, ref_col, pos_col, alt_col = \
        get_ih_table_and_change_cols(change_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
                select
                    s.id,
                    s.accession,
                    v.alt_freq_range::text,
                    lower(v.alt_freq_range)::double precision,
                    upper(v.alt_freq_range)::double precision
                from {ih_table} v
                inner join {catalog_table} c on c.id = v.{change_id_col}
                cross join lateral unnest(rb_to_array(v.{ColumnNames.samples_present})) as u({ColumnNames.sample_id})
                inner join {TableNames.samples} s on s.id = u.{ColumnNames.sample_id}
                where c.{feature_col} = :feature
                  and c.{ref_col} = :ref
                  and c.{pos_col} = :position
                  and c.{alt_col} = :alt
                order by lower(v.alt_freq_range), s.id
                """
            ),
            {'feature': feature, 'ref': ref, 'position': position, 'alt': alt},
        )
    return [
        VariantFreqInfo(
            sample_id=r[0],
            accession=r[1],
            alt_freq_range=r[2],
            alt_freq_lower=r[3],
            alt_freq_upper=r[4],
        )
        for r in res
    ]


async def get_mutation_sample_count_by_nt(change: str) -> List[MutationCountInfo]:
    region, ref_nt, position_nt, alt_nt = parse_change_string(change)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
                  select rb_cardinality(m.{ColumnNames.samples_present}) as sample_count
                  from {TableNames.cns_samples_by_allele} m
                  inner join {TableNames.alleles} a on a.id = m.{ColumnNames.allele_id}
                  where a.region = :region
                    and a.ref_nt = :ref_nt
                    and a.position_nt = :position_nt
                    and a.alt_nt = :alt_nt
                  """
            ),
            {'region': region, 'ref_nt': ref_nt, 'position_nt': position_nt, 'alt_nt': alt_nt}
        )
    return [
        MutationCountInfo(amino_sub_id=None, sample_count=r.sample_count) for r in res
    ]


async def get_mutation_sample_count_by_aa(change: str) -> List[MutationCountInfo]:
    gff_feature, ref_aa, position_aa, alt_aa = parse_change_string(change)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
                  select
                      mt.{ColumnNames.amino_acid_id} as amino_sub_id,
                      rb_cardinality(mt.{ColumnNames.samples_present}) as sample_count
                  from {TableNames.cns_samples_by_amino_acid} mt
                  inner join {TableNames.amino_acids} aa on aa.id = mt.{ColumnNames.amino_acid_id}
                  where aa.gff_feature = :gff_feature
                    and aa.ref_aa = :ref_aa
                    and aa.position_aa = :position_aa
                    and aa.alt_aa = :alt_aa
                  """
            ),
            {'gff_feature': gff_feature, 'ref_aa': ref_aa, 'position_aa': position_aa, 'alt_aa': alt_aa}
        )
    return [
        MutationCountInfo(amino_sub_id=r.amino_sub_id, sample_count=r.sample_count)
        for r in res
    ]

async def get_pheno_values_and_mutation_counts(
    pheno_metric_name: str, region: str, include_refs: bool, filter: str | None
) -> List["VariantCountPhenoScoreInfo"]:
    no_refs_filter = "and aas.ref_aa <> aas.alt_aa"
    if include_refs:
        no_refs_filter = ""

    if filter is None:
        query = f"""
            select aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value,
                   rb_cardinality(rb_or_agg(m.{ColumnNames.samples_present})) as count
            from {TableNames.cns_samples_by_amino_acid} m
            inner join {TableNames.amino_acids} aas on aas.id = m.{ColumnNames.amino_acid_id}
            inner join {TableNames.phenotype_metric_values} pmv on pmv.{ColumnNames.amino_acid_id} = aas.id
            inner join {TableNames.phenotype_metrics} pm on pm.id = pmv.{ColumnNames.phenotype_metric_id}
            where aas.gff_feature = :region
            and pm.{ColumnNames.phenotype_metric_name} = :pm_name
            {no_refs_filter}
            group by aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value
            order by count desc;
        """
    else:
        user_defined_filter = parser.parse(filter)
        query = f"""
            select aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value,
                   count(distinct caas.{ColumnNames.sample_id}) as count
            from {TableNames.cns_amino_acids_by_sample} caas
            cross join lateral unnest(rb_to_array(caas.{ColumnNames.amino_acids_present})) as u({ColumnNames.amino_acid_id})
            inner join {TableNames.amino_acids} aas on aas.id = u.{ColumnNames.amino_acid_id}
            inner join {TableNames.phenotype_metric_values} pmv on pmv.{ColumnNames.amino_acid_id} = aas.id
            inner join {TableNames.phenotype_metrics} pm on pm.id = pmv.{ColumnNames.phenotype_metric_id}
            where caas.{ColumnNames.sample_id} in (
                select s.id
                from {TableNames.samples} s
                left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
                where {user_defined_filter}
            )
            and aas.gff_feature = :region
            and pm.{ColumnNames.phenotype_metric_name} = :pm_name
            {no_refs_filter}
            group by aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value
            order by count desc;
        """

    async with get_async_session() as session:
        res = await session.execute(
            text(query),
            {"region": region, "pm_name": pheno_metric_name},
        )

    out_data = []
    for r in res:
        count = r[4]
        if count > 0:
            out_data.append(
                VariantCountPhenoScoreInfo(
                    ref_aa=r[0],
                    position_aa=r[1],
                    alt_aa=r[2],
                    pheno_value=r[3],
                    count=count,
                )
            )
    return out_data


# TODO: Using "region" as the parameter for "gff_feature" for now.
async def get_pheno_values_and_variant_counts(
    pheno_metric_name: str, region: str, include_refs: bool, filter: str | None
) -> List["VariantCountPhenoScoreInfo"]:
    no_refs_filter = "and aas.ref_aa <> aas.alt_aa"
    if include_refs:
        no_refs_filter = ""

    if filter is None:
        sample_subset_cte = ""
        count_expr = f"rb_or_cardinality_agg(v.{ColumnNames.samples_present})"
    else:
        sample_subset_cte = f"""
            with sample_subset_bm as (
                select coalesce(rb_build_agg(s.id), rb_build('{{}}')) as bm
                from {TableNames.samples} s
                left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
                where {parser.parse(filter)}
            )
        """
        count_expr = (
            f"rb_and_cardinality(rb_or_agg(v.{ColumnNames.samples_present}), "
            f"(select bm from sample_subset_bm))"
        )

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
                {sample_subset_cte}
                select aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value,
                       {count_expr} as count
                from {TableNames.ih_samples_by_amino_acid} v
                inner join {TableNames.amino_acids} aas on aas.id = v.{ColumnNames.amino_acid_id}
                inner join {TableNames.phenotype_metric_values} pmv on pmv.{ColumnNames.amino_acid_id} = aas.id
                inner join {TableNames.phenotype_metrics} pm on pm.id = pmv.{ColumnNames.phenotype_metric_id}
                where aas.gff_feature = :region
                and pm.{ColumnNames.phenotype_metric_name} = :pm_name
                {no_refs_filter}
                group by aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value
                order by count desc;
                """
            ),
            {"region": region, "pm_name": pheno_metric_name},
        )

    out_data = []
    for r in res:
        count = r[4]
        if count > 0:
            out_data.append(
                VariantCountPhenoScoreInfo(
                    ref_aa=r[0],
                    position_aa=r[1],
                    alt_aa=r[2],
                    pheno_value=r[3],
                    count=count,
                )
            )
    return out_data
