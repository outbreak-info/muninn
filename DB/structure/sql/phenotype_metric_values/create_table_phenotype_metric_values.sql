create table phenotype_metric_values (
	id bigserial not null,
	phenotype_metric_id bigint not null,
	amino_acid_id integer not null,
	value double precision not null
);
