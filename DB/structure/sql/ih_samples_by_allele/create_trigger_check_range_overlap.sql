do
$$
    begin
        if not exists (
            select *
            from information_schema.triggers
            where event_object_table = 'ih_samples_by_allele'
              and trigger_name = 'check_freq_range_overlap_ih_samples_by_allele'
        )
        then
            create trigger check_freq_range_overlap_ih_samples_by_allele
                before insert or update
                on ih_samples_by_allele
                for each row
            execute procedure check_freq_range_overlap_ih_samples_by_allele();
        end if;
    end;
$$;