alter table ih_samples_by_allele add constraint pk_ih_samples_by_allele
primary key (allele_id, alt_freq_range);