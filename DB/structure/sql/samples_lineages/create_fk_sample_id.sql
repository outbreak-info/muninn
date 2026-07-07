alter table samples_lineages add constraint fk_samples_lineages_sample_id_samples
foreign key (sample_id) references samples (id);
