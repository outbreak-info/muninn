create table if not exists cache_cns_pmv_sums (
    pmv_value_sum       double precision,
    n_scored_mutations  int,
    sample_id           int,
    phenotype_metric_id int,
    gff_feature         text
);