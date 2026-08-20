create or replace function check_sample_uq_within_amino_acid() returns trigger
    language plpgsql
as
$$
declare
    n_overlaps int;
begin
    select count(*)
    into n_overlaps
    from ih_samples_by_amino_acid isa
    where isa.amino_acid_id = new.amino_acid_id
      and isa.samples_present && new.samples_present
      and isa.alt_freq_range <> new.alt_freq_range;
    if n_overlaps > 0 then
        raise exception 'sample_id already present for same amino_acid_id in different alt_freq_range';
    end if;
    return new;
end;
$$;
