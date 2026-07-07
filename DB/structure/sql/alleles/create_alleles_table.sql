create table alleles (
	id serial not null,
	region text not null,
	position_nt bigint not null,
	ref_nt text not null,
	alt_nt text not null
);