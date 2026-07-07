alter table consensus_sequences_by_allele add constraint fk_consensus_sequences_by_allele_allele_id_alleles
foreign key (allele_id) references alleles (id);