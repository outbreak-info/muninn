alter table amino_acids add constraint ck_amino_acids_alt_aa_not_empty
check alt_aa <> '';