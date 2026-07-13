alter table intra_host_variants add constraint fk_intra_host_variants_allele_id_alleles
foreign key (allele_id) references alleles (id);
