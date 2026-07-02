alter table samples add constraint ck_samples_retraction_values_existence_in_harmony
check (not is_retracted and retraction_detected_date is null)
or (is_retracted and retraction_detected_date is not null);