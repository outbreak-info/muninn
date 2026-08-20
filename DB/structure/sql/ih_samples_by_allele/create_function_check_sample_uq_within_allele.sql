create or replace function check_sample_uq_within_allele() returns trigger
    language plpgsql
as
$$
declare
    n_overlaps int;
begin
    select count(*)
    into n_overlaps
    from ih_samples_by_allele isa
    where isa.allele_id = new.allele_id
      and isa.samples_present && new.samples_present
      and isa.alt_freq_range <> new.alt_freq_range;
    if n_overlaps > 0 then
        raise exception 'sample_id already present for same allele_id in different alt_freq_range';
    end if;
    return new;
end;
$$;