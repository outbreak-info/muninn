-- create user flu with superuser password 'h5n1';
-- drop database if exists flu;
-- create database flu with owner flu;
-- alter role flu superuser;

-- set role flu;

drop type if exists consent_level;
drop type if exists nucleotide;
drop type if exists amino_acid;
drop type if exists flu_region;
drop type if exists codon;
drop table if exists metadata;
drop table if exists mutations;
drop table if exists variants;
drop table if exists demixed;
drop table if exists lineages;
drop table if exists demixed;
drop table if exists demixed_lineages;


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

create table if not exists samples (
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

	gff_feature text not null,
	constraint no_empty_strings check (gff_feature <> ''),
	position_nt integer not null,
	alt_nt nucleotide,
	alt_nt_indel text,
	constraint no_empty_strings check (alt_nt_indel <> ''),
	unique nulls not distinct (gff_feature, region, position_nt, alt_nt, alt_nt_indel),
	constraint must_have_nt_alt_xor_indel check ((alt_nt is null) <> (alt_nt_indel is null))
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
