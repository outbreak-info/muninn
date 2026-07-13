alter table lineages_immediate_children add constraint fk_lineages_immediate_children_child_id_lineages
foreign key (parent_id) references lineages (id);