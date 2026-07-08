from sqlalchemy import text
from DB.engine import get_async_session
from api.models import StructuralAnnotationMutationsInfo, MutationWithCountInfo
from api.models import StructuralAnnotationMutationsInfo, MutationWithCountInfo, StructuralAnnotationMutationsTimelineInfo, StructuralAnnotationIntraHostVariantsTimelineInfo, MutationTimelineInfo

async def get_mutations_by_sequential_site(sequential_site: int, gff_feature: str = 'XAJ25415.1') -> StructuralAnnotationMutationsInfo:
    """Get all mutations at a given sequential site with their sample counts."""
    
    query = """
    SELECT 
      aa.ref_aa || aa.position_aa || aa.alt_aa AS mutation,
      COUNT(DISTINCT ihv.sample_id) AS sample_count
    FROM amino_acids aa
    JOIN intra_host_translations iht ON iht.amino_acid_id = aa.id
    JOIN intra_host_variants ihv ON ihv.id = iht.intra_host_variant_id
    WHERE aa.position_aa = :sequential_site 
      AND aa.gff_feature = :gff_feature
    GROUP BY aa.id, aa.ref_aa, aa.position_aa, aa.alt_aa
    ORDER BY sample_count DESC;
    """
    
    async with get_async_session() as session:
        result = await session.execute(
            text(query),
            {"sequential_site": sequential_site, "gff_feature": gff_feature}
        )
        rows = result.fetchall()
    
    mutations = [
        MutationWithCountInfo(mutation=row[0], sample_count=row[1])
        for row in rows
    ]
    
    return StructuralAnnotationMutationsInfo(
        sequential_site=sequential_site,
        mutations=mutations
    )

async def get_mutations_timeline_by_sequential_site(sequential_site: int, gff_feature: str = 'XAJ25415.1') -> StructuralAnnotationMutationsTimelineInfo:
    """Get mutations at a sequential site with dates they were observed."""
    
    query = """
    SELECT 
      aa.ref_aa || aa.position_aa || aa.alt_aa AS mutation,
      COUNT(DISTINCT m.sample_id) AS sample_count,
      ARRAY_AGG(DISTINCT s.collection_start_date ORDER BY s.collection_start_date) AS observed_dates
    FROM amino_acids aa
    JOIN mutation_translations mt ON mt.amino_acid_id = aa.id
    JOIN mutations m ON m.id = mt.mutation_id
    JOIN samples s ON s.id = m.sample_id
    WHERE aa.position_aa = :sequential_site 
      AND aa.gff_feature = :gff_feature
    GROUP BY aa.id, aa.ref_aa, aa.position_aa, aa.alt_aa
    ORDER BY sample_count DESC;
    """
    
    async with get_async_session() as session:
        result = await session.execute(
            text(query),
            {"sequential_site": sequential_site, "gff_feature": gff_feature}
        )
        rows = result.fetchall()
    
    mutations = [
        MutationTimelineInfo(
            mutation=row[0], 
            sample_count=row[1],
            observed_dates=[str(d) for d in row[2] if d is not None]
        )
        for row in rows
    ]
    
    return StructuralAnnotationMutationsTimelineInfo(
        sequential_site=sequential_site,
        gff_feature=gff_feature,
        mutations=mutations
    )


async def get_intra_host_variants_timeline_by_sequential_site(sequential_site: int, gff_feature: str = 'XAJ25415.1') -> StructuralAnnotationIntraHostVariantsTimelineInfo:
    """Get intra_host_variants at a sequential site with dates they were observed."""
    
    query = """
    SELECT 
      aa.ref_aa || aa.position_aa || aa.alt_aa AS mutation,
      COUNT(DISTINCT ihv.sample_id) AS sample_count,
      ARRAY_AGG(DISTINCT s.collection_start_date ORDER BY s.collection_start_date) AS observed_dates
    FROM amino_acids aa
    JOIN intra_host_translations iht ON iht.amino_acid_id = aa.id
    JOIN intra_host_variants ihv ON ihv.id = iht.intra_host_variant_id
    JOIN samples s ON s.id = ihv.sample_id
    WHERE aa.position_aa = :sequential_site 
      AND aa.gff_feature = :gff_feature
    GROUP BY aa.id, aa.ref_aa, aa.position_aa, aa.alt_aa
    ORDER BY sample_count DESC;
    """
    
    async with get_async_session() as session:
        result = await session.execute(
            text(query),
            {"sequential_site": sequential_site, "gff_feature": gff_feature}
        )
        rows = result.fetchall()
    
    mutations = [
        MutationTimelineInfo(
            mutation=row[0], 
            sample_count=row[1],
            observed_dates=[str(d) for d in row[2] if d is not None]
        )
        for row in rows
    ]
    
    return StructuralAnnotationIntraHostVariantsTimelineInfo(
        sequential_site=sequential_site,
        gff_feature=gff_feature,
        intra_host_variants=mutations
    )