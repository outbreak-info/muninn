alter table annotations_amino_acids add constraint fk_annotations_amino_acids_amino_acid_id_amino_acids
foreign key (amino_acid_id) references amino_acids (id);
