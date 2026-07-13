create table samples (
	id serial not null,
	accession text not null,

	bio_project text,
	bio_sample text,
	bio_sample_accession text,
	bio_sample_model text,
	center_name text,
	experiment text,

	host text,

	instrument text,
	platform text,
	isolate text,

	library_name text,
	library_layout text,
	library_selection text,
	library_source text,
	organism text,
	is_retracted boolean not null,
	retraction_detected_date timestamp with time zone,
	isolation_source text,

	collection_start_date date,
	collection_end_date date,
	release_date timestamp with time zone,
	creation_date timestamp with time zone,

	version text,
	sample_name text,
	sra_study text,
	serotype text,

	geo_location_id integer,
	assay_type text,
	avg_spot_length double precision,
	bases bigint,

	-- wastewater columns
	ww_viral_load double precision,
	ww_catchment_population bigint,
	ww_site_id text,
	ww_collected_by text,
	census_region text
);

