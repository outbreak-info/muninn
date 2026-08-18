create materialized view cache_cns_pmv_sums as
with scored as (
    select pmv.phenotype_metric_id,
           pmv.value as value,
           sample_id
    from phenotype_metric_values pmv
    inner join cns_samples_by_amino_acid csaa using (amino_acid_id)
    cross join lateral unnest(rb_to_array(csaa.samples_present)) as u(sample_id)
)
select sum(value) as pmv_value_sum,
       count(*) as n_scored_mutations,
       sample_id,
       phenotype_metric_id
from scored
inner join samples s on s.id = scored.sample_id
where num_nulls(s.collection_end_date, s.collection_start_date) = 0
group by sample_id, phenotype_metric_id;