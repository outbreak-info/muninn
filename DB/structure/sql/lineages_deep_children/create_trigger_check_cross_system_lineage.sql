do
$$
	begin
		if not exists (
			select *
			from information_schema.triggers
			where event_object_table = 'lineages_immediate_children'
			  and trigger_name = 'check_cross_system_lineage_trigger'
		)
		then
			create trigger check_cross_system_lineage_trigger
				before insert or update
				on lineages_immediate_children
				for each row
			execute procedure check_cross_system_lineage();
		end if;
	end;
$$;