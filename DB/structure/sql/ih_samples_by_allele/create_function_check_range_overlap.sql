create or replace function check_freq_range_overlap_ih_samples_by_allele()
	returns trigger as
$$
declare
	n_overlaps int;
begin
	select count(*)
	into n_overlaps
	from ih_samples_by_allele isa
	where isa.allele_id = new.allele_id
		and isa.alt_freq_range && new.alt_freq_range
		and isa.alt_freq_range <> new.alt_freq_range;
	if n_overlaps > 0 then
		raise exception 'alt_freq_range overlaps existing record with same allele_id';
	end if;
	return new;
end;
$$
	language plpgsql;