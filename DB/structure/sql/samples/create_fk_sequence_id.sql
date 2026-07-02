alter table samples add constraint fk_samples_sequence_id_sequences
foreign key (sequence_id) references sequences (id);