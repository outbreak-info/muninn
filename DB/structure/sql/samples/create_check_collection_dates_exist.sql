alter table samples add constraint ck_samples_collection_start_and_end_both_absent_or_both_present
check num_nulls(collection_start_date, collection_end_date) in (0, 2);
