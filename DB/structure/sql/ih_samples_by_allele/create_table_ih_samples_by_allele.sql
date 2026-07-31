create table ih_samples_by_allele (
	allele_id integer not null,
	alt_freq_range numrange not null,
	samples_present roaringbitmap not null
);
