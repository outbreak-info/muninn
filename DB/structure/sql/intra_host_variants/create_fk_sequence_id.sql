alter table intra_host_variants add constraint fk_intra_host_variants_sequence_id_sequences
foreign key (sequence_id) references sequences (id);
