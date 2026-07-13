alter table lineages_immediate_children add constraint ck_lineages_immediate_children_no_self_parenthood
check (parent_id <> child_id);