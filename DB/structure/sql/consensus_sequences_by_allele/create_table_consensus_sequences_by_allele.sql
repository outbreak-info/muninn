create table consensus_sequences_by_allele (
	allele_id integer not null,
	sequences_present roaringbitmap storage extended not null
);