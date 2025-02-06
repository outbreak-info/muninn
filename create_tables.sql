create type consent_level as enum ('public', 'other');

create type nucleotide as enum ('A', 'C', 'G', 'T', 'U', 'N');

create type amino_acid as enum (
	'A', 'C', 'D', 'E',
	'F', 'G', 'H', 'I',
	'K', 'L', 'M', 'N',
	'P', 'Q', 'R', 'S',
	'T', 'V', 'W', 'Y', 'STAR'
);

create type flu_region as enum (
	'HA', 'NA',
	'NP', 'MP',
	'PB1', 'PB2',
	'NS', 'PB1-F2',
	'PA', 'PA-X',
	'M1', 'M2',
	'NS1', 'NS2'

);

create type codon as enum (
	'AAA', 'AAC', 'AAG', 'AAT',
	'ACA', 'ACC', 'ACG', 'ACT',
	'AGA', 'AGC', 'AGG', 'AGT',
	'ATA', 'ATC', 'ATG', 'ATT',
	'CAA', 'CAC', 'CAG', 'CAT',
	'CCA', 'CCC', 'CCG', 'CCT',
	'CGA', 'CGC', 'CGG', 'CGT',
	'CTA', 'CTC', 'CTG', 'CTT',
	'GAA', 'GAC', 'GAG', 'GAT',
	'GCA', 'GCC', 'GCG', 'GCT',
	'GGA', 'GGC', 'GGG', 'GGT',
	'GTA', 'GTC', 'GTG', 'GTT',
	'TAA', 'TAC', 'TAG', 'TAT',
	'TCA', 'TCC', 'TCG', 'TCT',
	'TGA', 'TGC', 'TGG', 'TGT',
	'TTA', 'TTC', 'TTG', 'TTT'
);

create table samples (
	id bigserial primary key,
	accession text not null
);

create table mutations (
	id bigserial primary key,
	position_aa integer,
	ref_aa amino_acid,
	alt_aa amino_acid,
	region flu_region,
	unique(region, position_aa, ref_aa, alt_aa),

	gff_feature text,
	check (gff_feature <> ''),
	position_nt integer not null,
	alt_nt nucleotide,
	alt_nt_indel text,
	check (alt_nt_indel <> ''),
	unique nulls not distinct (gff_feature, region, position_nt, alt_nt, alt_nt_indel),
	constraint must_have_nt_alt_xor_indel check num_nulls(alt_nt, alt_nt_indel) = 1
);


create table intra_host_variants (
	sample_id bigint references samples (id) not null,
	mutation_id bigint references mutations (id) not null,
	primary key (sample_id, mutation_id),

	ref_dp integer not null,
	alt_dp integer not null,
	alt_freq double precision not null
);

create table dms_results (
	id bigserial primary key,
	mutation_id bigint references mutations (id) not null,
	ferret_sera_escape double precision not null,
	stability double precision not null
);
