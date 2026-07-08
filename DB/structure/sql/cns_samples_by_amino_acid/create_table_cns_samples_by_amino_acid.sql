create table cns_samples_by_amino_acid (
	amino_acid_id integer not null,
	samples_present roaringbitmap not null
);