alter table intra_host_variants add constraint fk_intra_host_variants_sample_id_samples
foreign key (sample_id) references samples (id);
