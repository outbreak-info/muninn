from sqlalchemy import text
from DB.engine import get_async_session
from api.models import StructuralAnnotationMutationsInfo, MutationWithCountInfo


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