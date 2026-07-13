alter table lineages add constraint fk_lineages_lineage_system_id_lineage_systems
foreign key (lineage_system_id) references lineage_systems (id);
