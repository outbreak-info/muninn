alter table phenotype_metrics add constraint ck_phenotype_metrics_name_not_empty
check (phenotype_metric_name <> '');
