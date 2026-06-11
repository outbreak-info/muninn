import asyncio

from DB.queries.lineages import count_samples_by_lineage, get_mutation_incidence
from utils.constants import NtOrAa, MIN_PREVALENCE_THRESHOLD, DEFAULT_PREVALENCE_THRESHOLD


class AlleleIncidenceByLineageCache:
    cachesByLineageSystem = dict()

    @classmethod
    def get_cache(cls, lineage_system_name: str):
        if lineage_system_name is None:
            raise TypeError
        try:
            return cls.cachesByLineageSystem[lineage_system_name]
        except KeyError:
            new_cache = AlleleIncidenceByLineageCache(lineage_system_name)
            cls.cachesByLineageSystem[lineage_system_name] = new_cache
            return new_cache

    def __init__(self, lineage_system_name: str):
        self.lineage_system_name = lineage_system_name
        self.data = dict()
        self.populated = False
        self.populating = False

    async def populate(self):
        if self.populating or self.populated:
            return
        self.populating = True

        lineage_sample_counts = await count_samples_by_lineage(self.lineage_system_name)
        lineages_to_cache = [lsc['lineage_name'] for lsc in lineage_sample_counts if lsc['n_samples'] > 0] # todo
        for lineage in lineages_to_cache:
            result = await get_mutation_incidence(
                lineage,
                self.lineage_system_name,
                NtOrAa.nt,
                MIN_PREVALENCE_THRESHOLD,
                True,
                None
            )
            self.data[lineage] = result

        self.populating = False
        self.populated = True

    @classmethod
    def answer_from_cache(
        cls,
        lineage_system_name: str,
        lineage_name: str,
        prevalence_threshold: float = DEFAULT_PREVALENCE_THRESHOLD,
        match_reference: bool = False
    ) -> dict:
        stored = cls.cachesByLineageSystem[lineage_system_name].data[lineage_name]
        mutation_counts = stored['mutation_counts']

        filtered = {region: [m for m in muts if m['prevalence'] >= prevalence_threshold] for region, muts in
                    mutation_counts.items()}
        if not match_reference:
            filtered = {region: [m for m in muts if m['ref'] != m['alt']] for region, muts in filtered.items()}
        return {
            'sample_count': stored['sample_count'],
            'mutation_counts': filtered
        }
