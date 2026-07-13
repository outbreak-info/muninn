create table samples_lineages (
	id bigserial not null,
	sample_id integer not null,
	lineage_id bigint not null,
	abundance double precision,
	is_consensus_call boolean not null
);
