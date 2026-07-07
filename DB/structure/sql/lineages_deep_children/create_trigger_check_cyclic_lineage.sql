do
$$
	begin
		if not exists (
			select *
			from information_schema.triggers
			where event_object_table = 'lineages_immediate_children'
			  and trigger_name = 'check_cyclic_lineage_trigger'
		)
		then
			create trigger check_cyclic_lineage_trigger
				before insert or update
				on lineages_immediate_children
				for each row
			execute procedure check_cyclic_lineage();
		end if;
	end;
$$;