create or replace function check_cross_system_lineage()
	returns trigger as
$$
declare
	num_systems int;
begin
	select count(distinct(lineage_system_id))
	into num_systems
	from lineages l
	where l.id = new.parent_id or l.id = new.child_id;
	if num_systems > 1 then
		raise exception 'parent and child are from different lineage systems';
	end if;
	return new;
end;
$$
	language plpgsql;