create or replace view lineages_deep_children as
with recursive deep_children(parent_id, child_id) as (
	select lic.parent_id,
		   lic.child_id
	from lineages_immediate_children lic
	union all
	select dc.parent_id,
		   lic.child_id
	from deep_children dc
	inner join lineages_immediate_children lic on dc.child_id = lic.parent_id
)
select parent_id, child_id
from deep_children;