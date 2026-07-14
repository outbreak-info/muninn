alter table ih_samples_by_amino_acid add constraint pk_ih_samples_by_amino_acid
primary key (amino_acid_id, alt_freq_range);