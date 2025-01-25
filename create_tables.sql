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
	'T', 'V', 'W', 'Y', '*'
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
	assay_type text not null,
	avg_spot_len double precision not null, -- todo
	bases bigint not null,
	bio_project text not null,
	bio_sample text, -- nullable
	bio_sample_accession text not null,
	bio_sample_model text not null,
	bytes bigint not null,
	center_name text not null,
	collection_date date not null,
	consent consent_level not null,
	create_date date not null,
	datastore_filetype text not null,
	datastore_provider text not null,
	datastore_region text not null,
	experiment text not null,
	geo_loc_name text not null, -- todo
	geo_loc_name_country text not null, -- todo
	geo_loc_name_country_continent text, -- nullable todo look into if there's a slick way to do this
	host text not null,
	instrument text not null, 
	isolate text not null, 
	isolation_source text, -- nullable
	library_layout text not null, 
	library_name text not null, 
	library_selection text not null, 
	library_source text not null, 
	organism text not null,
	platform text not null,
	release_date date not null,
	run text not null,
	sample_name text not null,
	serotype text not null, -- todo
	sra_study text not null,
	version text not null -- ???
);

create table dms_results (
	id bigserial primary key,
	antibody_set text not null,
	entry_in_293_t_cells real not null, 
	ferret_sera_escape real not null, 
	ha1_ha2_h5_site text not null, 
	mature_h5_site real not null,
	mouse_sera_escape real not null,
	nt_changes_to_codon real not null,
	reference_h1_site bigint not null, 
	region flu_region not null, -- this is REGION from es
	region_other text not null, -- this is region from es
	sa26_usage_increase real not null,
	sequential_site real not null,
	species_sera_escape real not null,
	stability real not null
);

create table mutations (
	id bigserial primary key,
	metadata_id bigint references metadata (id) not null, -- replaces sra
	position_nt bigint not null,
	ref_nt nucleotide not null, 
	alt_nt nucleotide not null,
	position_aa integer not null,
	ref_aa amino_acid not null,
	alt_aa amino_acid not null,
	region flu_region not null
);

create table variants (
	id bigserial primary key,
	metadata_id bigint references metadata (id) not null, -- replaces sra

	position bigint not null, -- was pos
	ref_nt nucleotide not null, -- was ref
	alt_nt nucleotide not null, -- was alt
	ref_codon codon not null, -- todo
	alt_codon codon not null, -- could make an enum?

	position_aa double precision not null, -- was pos_aa
	ref_aa amino_acid not null,
	alt_aa amino_acid not null,

	ref_dp integer not null, -- todo
	alt_dp integer not null, -- todo name
	ref_qual double precision not null, -- todo
	alt_qual double precision not null,
	ref_rv integer not null, 
	alt_rv integer not null,
	alt_freq double precision not null,
	pass boolean not null,
	pval double precision not null,
	total_dp integer not null,
	dms_result_id bigint references dms_results (id)
);

-- create table demixed (
-- 	id bigserial primary key,
-- 	metadata_id bigint references metadata (id) not null, -- replaces sra
-- 	abundances real[] not null,
-- 	coverage real not null,
-- 	filename text not null,
-- 	lineages text[] not null,
-- 	resid real not null, -- todo: unabbreviate name?
-- 	summarized_score real not null,
-- 	variants_filename text not null
-- );


-- let's try a fancier version of this?

-- eventually this could have parents and implement a DAG via check constraint?
create table lineages (
	id bigserial primary key,
	name text not null,
	alias text not null
);

create table demixed (
	id bigserial primary key,
	metadata_id bigint references metadata (id) not null, -- replaces sra
	coverage real not null,
	filename text not null, -- maybe don't need
	resid real not null, -- todo: unabbreviate name?
	summarized_score real not null,
	variants_filename text not null
);

create table demixed_lineages (
	demixed_id bigint references demixed (id) not null,
	lineage_id bigint references lineages (id) not null,
	demixed_position integer not null, -- this saves this entry's position in the array
	abundance real not null,
	primary key (demixed_id, lineage_id)
);
