create table intra_host_variants (
	sequence_id integer not null,
	allele_id integer not null,
	ref_dp bigint not null,
    alt_dp bigint not null,
    alt_freq double precision not null,
    ref_rv bigint not null,
    alt_rv bigint not null,
    ref_qual bigint not null,
    alt_qual bigint not null,
    total_dp bigint not null,
    pval double precision not null,
    pass_qc boolean not null
);