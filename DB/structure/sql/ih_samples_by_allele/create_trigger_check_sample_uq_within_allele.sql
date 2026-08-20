do
$$
    begin
        if not exists (
            select *
            from information_schema.triggers
            where event_object_table = 'ih_samples_by_allele'
              and trigger_name = 'check_sample_uq_within_allele'
        )
        then
            create trigger check_sample_uq_within_allele
                before insert or update
                on ih_samples_by_allele
                for each row
            execute procedure check_sample_uq_within_allele();
        end if;
    end;
$$;