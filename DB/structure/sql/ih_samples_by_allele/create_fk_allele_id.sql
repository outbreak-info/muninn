alter table ih_samples_by_allele add constraint fk_ih_samples_by_allele_allele_id_alleles
foreign key (allele_id) references alleles (id);