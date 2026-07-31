alter table cns_alleles_by_sample add constraint fk_alleles_by_sample_sample_id_samples
foreign key (sample_id) references samples (id);