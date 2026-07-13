alter table samples add constraint fk_samples_geo_location_id_geo_locations
foreign key (geo_location_id) references geo_locations (id);