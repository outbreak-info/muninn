alter table amino_acids add constraint ck_amino_acids_ref_codon_not_empty
check ref_codon <> '';