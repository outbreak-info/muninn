alter table phenotype_metric_values add constraint fk_phenotype_metric_values_amino_acid_id_amino_acids
foreign key (amino_acid_id) references amino_acids (id);
