create table ih_samples_by_amino_acid (
	amino_acid_id integer not null,
	alt_freq_range numrange not null,
	samples_present roaringbitmap not null
);