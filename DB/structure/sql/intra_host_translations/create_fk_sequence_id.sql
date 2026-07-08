alter table intra_host_translations add constraint fk_intra_host_translations_sequence_id_sequences
foreign key (sequence_id) references sequences (id);
