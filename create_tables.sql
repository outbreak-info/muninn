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

-- My guess is that things we want to use for our analysis internally
-- we'll want to store as first class things. Stuff that we just have as
-- data to spit back out can live in strings. 
-- if we store data as just strings we risk problems with silly mismatching
-- some of these should probably be unique not null, but I'm not sure which
-- todo: sort out int/bigint and real/double
create table if not exists metadata (
	id bigserial primary key,
	run text not null
);

create table mutations (
	id bigserial primary key,
	position_nt bigint,
	ref_nt nucleotide,
	alt_nt nucleotide,
	position_aa integer not null,
	ref_aa amino_acid not null,
	alt_aa amino_acid not null,
	region flu_region not null,
	unique(region, position_aa, ref_aa, alt_aa)
);

create table metadata_mutations (
    metadata_id bigint references metadata (id),
    mutation_id bigint references mutations (id),
    primary key (metadata_id, mutation_id)
);

/*
 do we need more "location" data associated with these results?
*/
create table dms_results (
	id bigserial primary key,
	antibody_set text not null,
	entry_in_293_t_cells double precision not null,
	ferret_sera_escape double precision not null,
	ha1_ha2_h5_site text not null,
	mature_h5_site double precision not null,
	mouse_sera_escape double precision not null,
	nt_changes_to_codon double precision not null,
	reference_h1_site bigint not null,
	region flu_region not null, -- this is REGION from es
	region_other text not null, -- this is region from es
	sa26_usage_increase double precision not null,
	sequential_site double precision not null,
	species_sera_escape double precision not null,
	stability double precision not null
);

create table variants (
	id bigserial primary key,
	metadata_id bigint references metadata (id) not null, -- replaces sra

	position_nt bigint not null, -- was pos
	ref_nt nucleotide not null, -- was ref
	alt_nt nucleotide, -- was alt
	alt_nt_indel text,
	-- Must have either alt nt or an indel, can't have both
	constraint must_have_nt_alt_xor_indel check ((alt_nt is null) >< (alt_nt_indel is null)),

	ref_codon codon,
	alt_codon codon,

	position_aa double precision, -- was pos_aa todo: should be int?
	ref_aa amino_acid,
	alt_aa amino_acid,

	gff_feature text,
	region flu_region not null,
	region_full_text text not null,

	ref_dp integer not null,
	alt_dp integer not null,
	ref_qual double precision not null,
	alt_qual double precision not null,
	ref_rv integer not null, 
	alt_rv integer not null,
	alt_freq double precision not null,
	pass boolean not null,
	pval double precision not null,
	total_dp integer not null,

	dms_result_id bigint references dms_results (id)
);

-- eventually this could have parents and implement a DAG via check constraint?
create table lineages (
	id bigserial primary key,
	name text not null,
	alias text not null
);

create table demixed (
	id bigserial primary key,
	metadata_id bigint references metadata (id) not null, -- replaces sra
	coverage double precision not null,
	filename text not null, -- maybe don't need
	resid double precision not null, -- todo: unabbreviate name?
	summarized_score double precision not null,
	variants_filename text not null
);

create table demixed_lineages (
	demixed_id bigint references demixed (id) not null,
	lineage_id bigint references lineages (id) not null,
	demixed_position integer not null, -- this saves this entry's position in the array todo: should have a better name?
	abundance double precision not null,
	primary key (demixed_id, lineage_id)
);
