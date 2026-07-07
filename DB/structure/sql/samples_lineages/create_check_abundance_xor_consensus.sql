alter table samples_lineages add constraint ck_samples_lineages_has_abundance_xor_is_consensus
check ((abundance is null) = is_consensus_call);
