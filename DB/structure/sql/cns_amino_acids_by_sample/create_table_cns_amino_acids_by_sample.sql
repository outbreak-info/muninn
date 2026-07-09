create table cns_amino_acids_by_sample (
	sample_id integer not null,
	amino_acids_present roaringbitmap not null
);