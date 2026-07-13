alter table amino_acids add constraint uq_amino_acids_gff_feature_position_alt_aa_alt_codon
unique (gff_feature, alt_aa, alt_codon) include (id);