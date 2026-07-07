create table consensus_sequences_by_amino_acid (
	amino_acid_id integer not null,
	sequences_present roaringbitmap not null
);