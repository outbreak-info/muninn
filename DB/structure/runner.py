from DB.structure import sequences, samples, geo_locations, alleles, amino_acids, phenotype_metrics, \
    phenotype_metric_values, consensus_sequences_by_allele, consensus_sequences_by_amino_acid, lineage_systems, \
    lineages, samples_lineages, lineages_immediate_children, lineages_deep_children


async def set_up_db():
    await sequences.create_all()
    await geo_locations.create_all()
    await samples.create_all()
    await alleles.create_all()
    await amino_acids.create_all()

    await consensus_sequences_by_allele.create_all()
    await consensus_sequences_by_amino_acid.create_all()

    await phenotype_metrics.create_all()
    await phenotype_metric_values.create_all()

    await lineage_systems.create_all()
    await lineages.create_all()
    await samples_lineages.create_all()
    await lineages_immediate_children.create_all()
    await lineages_deep_children.create_all()