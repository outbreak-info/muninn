alter table lineages_immediate_children add constraint pk_lineages_immediate_children
primary key (parent_id, child_id);