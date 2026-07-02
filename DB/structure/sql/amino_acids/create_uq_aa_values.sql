alter table amino_acids add constraint uq_amino_acids_gff_feature_position_alt_aa_alt_codon
unique (alt_aa, gff_feature, alt_codon) include (id);