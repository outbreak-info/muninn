alter table cns_samples_by_allele add constraint fk_cns_samples_by_allele_allele_id_alleles
foreign key (allele_id) references alleles (id);