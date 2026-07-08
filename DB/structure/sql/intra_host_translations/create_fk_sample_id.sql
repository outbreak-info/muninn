alter table intra_host_translations add constraint fk_intra_host_translations_sample_id_samples
foreign key (sample_id) references samples (id);
