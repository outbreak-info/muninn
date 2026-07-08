create table cns_samples_by_allele (
	allele_id integer not null,
	samples_present roaringbitmap storage extended not null
);