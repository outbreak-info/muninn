do
$$
    begin
        if not exists (
            select *
            from information_schema.triggers
            where event_object_table = 'ih_samples_by_amino_acid'
              and trigger_name = 'check_sample_uq_within_amino_acid'
        )
        then
            create trigger check_sample_uq_within_amino_acid
                before insert or update
                on ih_samples_by_amino_acid
                for each row
            execute procedure check_sample_uq_within_amino_acid();
        end if;
    end;
$$;
