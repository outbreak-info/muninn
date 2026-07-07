alter table amino_acids add constraint ck_amino_acids_gff_feature_not_empty
check (gff_feature <> '');