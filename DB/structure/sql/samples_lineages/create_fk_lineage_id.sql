alter table samples_lineages add constraint fk_samples_lineages_lineage_id_lineages
foreign key (lineage_id) references lineages (id);
