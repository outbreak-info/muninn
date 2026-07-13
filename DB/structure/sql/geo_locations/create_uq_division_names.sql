alter table geo_locations add constraint uq_geo_locations_division_names
unique nulls not distinct (country_name, admin1_name, admin2_name, admin3_name);