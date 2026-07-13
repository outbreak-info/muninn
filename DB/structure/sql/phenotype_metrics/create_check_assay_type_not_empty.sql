alter table phenotype_metrics add constraint ck_phenotype_metrics_assay_type_not_empty
check (phenotype_metric_assay_type <> '');
