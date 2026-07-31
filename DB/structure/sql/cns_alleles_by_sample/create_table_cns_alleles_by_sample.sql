create table cns_alleles_by_sample (
	sample_id integer not null,
	alleles_present roaringbitmap not null
);