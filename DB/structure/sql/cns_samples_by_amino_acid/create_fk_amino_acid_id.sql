alter table cns_samples_by_amino_acid add constraint fk_cns_samples_by_amino_acid_amino_acid_id_amino_acids
foreign key (amino_acid_id) references amino_acids (id);
