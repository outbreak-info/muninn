alter table cns_amino_acids_by_sample add constraint fk_cns_amino_acids_by_sample_sample_id_samples
foreign key (sample_id) references samples (id);