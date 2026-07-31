alter table alleles add constraint uq_alleles_nt_values
unique (region, position_nt, alt_nt) include (id);