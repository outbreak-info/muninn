alter table lineages add constraint uq_lineages_name_uq_within_system
unique (lineage_system_id, lineage_name);
