from DB.structure import samples, geo_locations, alleles, amino_acids, phenotype_metrics, \
    phenotype_metric_values, cns_samples_by_allele, cns_samples_by_amino_acid, lineage_systems, \
    lineages, samples_lineages, lineages_immediate_children, lineages_deep_children, intra_host_variants, \
    intra_host_translations, effects, papers, annotations, annotations_papers, annotations_amino_acids, \
    cns_alleles_by_sample, cns_amino_acids_by_sample, ih_samples_by_allele


async def set_up_db():
    await geo_locations.create_all()
    await samples.create_all()
    await alleles.create_all()
    await amino_acids.create_all()

    await intra_host_variants.create_all()
    await intra_host_translations.create_all()

    await cns_samples_by_allele.create_all()
    await cns_alleles_by_sample.create_all()
    await cns_samples_by_amino_acid.create_all()
    await cns_amino_acids_by_sample.create_all()

    await ih_samples_by_allele.create_all()

    await phenotype_metrics.create_all()
    await phenotype_metric_values.create_all()

    await lineage_systems.create_all()
    await lineages.create_all()
    await samples_lineages.create_all()
    await lineages_immediate_children.create_all()
    await lineages_deep_children.create_all()

    await effects.create_all()
    await papers.create_all()
    await annotations.create_all()
    await annotations_papers.create_all()
    await annotations_amino_acids.create_all()
