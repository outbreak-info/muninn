from typing import List

from sqlalchemy import text
from sqlalchemy.orm import Session

from DB.engine import engine
from api.models import VariantCountPhenoScoreInfo


def get_pheno_values_and_variant_counts(pheno_metric_name: str, region: str) -> List['VariantCountPhenoScoreInfo']:
    with Session(engine) as session:
        res = session.execute(
            text(
                '''
                select aas.ref_aa, aas.position_aa, aas.alt_aa, pmr.value, (select 
                    count(1) from intra_host_variants ihv_by_allele where ihv_by_allele.allele_id = ihv.allele_id
                ) as count
                from (select distinct allele_id from intra_host_variants) ihv
                left join alleles a on a.id = ihv.allele_id
                left join translations t on t.allele_id = a.id
                left join amino_acid_substitutions aas on aas.id = t.amino_acid_substitution_id
                left join phenotype_measurement_results pmr on pmr.amino_acid_substitution_id = aas.id
                left join phenotype_metrics pm on pm.id = pmr.phenotype_metric_id
                where a.region = :region and pm.name = :pm_name
                order by count desc;
                '''
            ),
            {
                'region': region,
                'pm_name': pheno_metric_name
            }
        ).all()

    out_data = []
    for r in res:
        out_data.append(
            VariantCountPhenoScoreInfo(
                ref_aa=r[0],
                position_aa=r[1],
                alt_aa=r[2],
                pheno_value=r[3],
                count=r[4]
            )
        )
    return out_data
