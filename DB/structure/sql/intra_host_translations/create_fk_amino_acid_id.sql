alter table intra_host_translations add constraint fk_intra_host_translations_amino_acid_id_amino_acids
foreign key (amino_acid_id) references amino_acids (id);
