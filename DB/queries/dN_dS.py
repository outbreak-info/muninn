import re
import math
import statistics
from collections import defaultdict
from datetime import date
from typing import Dict, List, NamedTuple, Tuple

from DB.textutils import text

from DB.engine import get_async_session
from api.models import VariantDivPerSiteInfo
from utils.constants import ColumnNames, TableNames, CODONS_AMINO_ACIDS

CODON_TO_AA = dict(CODONS_AMINO_ACIDS)

S, NS, STOP = 'S', 'NS', 'Stop'
CLASSES = (NS, S, STOP)
METRIC_NAMES = {NS: 'dn', S: 'ds', STOP: 'dstop'}
HOST, POPULATION = 'Host', 'Population'

# Number of non-synonymous / synonymous / stop sites per GFF feature: {gff_feature: (NS, S, Stop)}.
N_SITES: Dict[str, Tuple[float, float, float]] = {
    'YP_009724389.1': (15639.666666677233, 4634.333333333589, 1014.0000000000454),
    'YP_009724390.1': (2802.6666666668234, 846.3333333333596, 170.00000000000045),
    'YP_009724391.1': (600.666666666665, 183.6666666666675, 40.66666666666668),
    'YP_009724392.1': (159.66666666666683, 57.333333333333464, 7.9999999999999964),
    'YP_009724393.1': (481.99999999999005, 156.3333333333334, 27.666666666666636),
    'YP_009724394.1': (138.66666666666623, 35.66666666666664, 8.666666666666664),
    'YP_009724395.1': (262.00000000000256, 83.66666666666656, 17.33333333333334),
    'YP_009724396.1': (265.0000000000024, 76.33333333333333, 21.666666666666657),
    'YP_009724397.2': (912.0000000000338, 288.0000000000011, 57.00000000000013),
    'YP_009725255.1': (86.66666666666652, 23.999999999999982, 3.3333333333333335),
    'YP_009725295.1': (9683.333333333065, 2904.3333333335363, 627.3333333333347),
    'YP_009725318.1': (95.99999999999972, 25.999999999999975, 6.999999999999997),
    'XAJ25415.1': (1264.333333333325, 352.66666666666407, 83.99999999999989), # HA
    'XAJ25416.1': (549.6666666666592, 176.6666666666673, 29.66666666666663), # MP
    'XAJ25417.1': (216.00000000000176, 62.00000000000016, 13.000000000000005), # MP
    'XAJ25418.1': (1049.0000000000407, 293.6666666666674, 64.3333333333335), # NA
    'XAJ25419.1': (1098.666666666696, 330.333333333332, 65.00000000000016), # NP
    'XAJ25420.1': (506.6666666666553, 157.33333333333343, 25.999999999999975), # NS
    'XAJ25421.1': (271.00000000000205, 72.33333333333339, 19.666666666666664), # NS
    'XAJ25422.1': (1600.9999999999152, 445.6666666666588, 101.33333333333297), # PA
    'XAJ25423.1': (562.3333333333273, 153.33333333333331, 40.33333333333334), # PA
    'XAJ25424.1': (1682.9999999998965, 481.99999999999005, 105.99999999999957), # PB1
    'XAJ25425.1': (204.33333333333476, 49.33333333333341, 16.333333333333343), # PB1
    'XAJ25426.1': (1667.6666666665667, 506.333333333322, 102.99999999999962), # PB2
}


_REGION_PATTERN = re.compile(
    r'^(?P<gff_feature>' + '|'.join(re.escape(k) for k in sorted(N_SITES, key=len, reverse=True)) + r')_(?P<reference>.+)$'
)


class _AaRow(NamedTuple):
    accession: str
    collection_date: date
    gff_feature: str
    position_aa: int
    ref_aa: str
    alt_aa: str
    ref_codon: str
    alt_codon: str
    alt_freq: float


async def get_divergence_per_site(
    region: str,
    lineage: str,
    hosts: List[str],
    location: str,
    n_samples: int,
    seed: int = 42
) -> List['VariantDivPerSiteInfo']:
    gff_feature = _REGION_PATTERN.match(region)["gff_feature"]

    ihv_rows, cns_rows = await _fetch_rows(region, lineage, hosts, location, n_samples, seed)
    ihv_rows = _reconstruct_ref_rows(ihv_rows)
    ihv_rows, cns_rows, day_by_sample = _cal_relative_day(ihv_rows, cns_rows)

    ihv_rows = _call_against_consensus(ihv_rows, cns_rows)
    cns_rows = [r._replace(alt_freq=1.0) for r in cns_rows if r.ref_aa != 'X' and r.alt_aa != 'X']

    # samples at relative day 0 are dropped to avoid div by 0
    # relative day is not used in calculation for now
    samples = sorted(acc for acc, day in day_by_sample.items() if day != 0)
    n_used = len(samples)

    out = []
    for level, rows in ((HOST, ihv_rows), (POPULATION, cns_rows)):
        res = _cal_divergence_per_site(rows, samples, gff_feature)
        for cls, per_site in res.items():
            out.append(VariantDivPerSiteInfo(
                region=region,
                lineage=lineage,
                host=hosts,
                location=location,
                n_samples=n_used,
                level=level,
                metric=METRIC_NAMES[cls],
                mean=statistics.fmean(per_site) if per_site else None,
                se=statistics.stdev(per_site) / math.sqrt(len(per_site)) if len(per_site) > 1 else None,
                n=len(per_site),
            ))
    return out


async def _fetch_rows(
    region: str,
    lineage: str,
    hosts: List[str],
    location: str,
    n_samples: int,
    seed: int = 42,
) -> Tuple[List[_AaRow], List[_AaRow]]:
    aa_columns = '''
        p.accession,
        p.collection_date,
        c.gff_feature,
        c.position_aa,
        c.ref_aa,
        c.alt_aa,
        c.ref_codon,
        c.alt_codon
    '''

    query = f'''
        with picked as (
            select
                s.id,
                s.{ColumnNames.accession} as accession,
                s.{ColumnNames.collection_start_date} as collection_date
            from {TableNames.samples} s
            left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
            where s.{ColumnNames.host} = any(:hosts)
            and gl.{ColumnNames.admin1_name} = :location
            and s.{ColumnNames.collection_start_date} is not null
            and not (
                extract(month from s.{ColumnNames.collection_start_date}) = 1
                and extract(day from s.{ColumnNames.collection_start_date}) = 1
            )
            and s.id in (
                select sl.{ColumnNames.sample_id}
                from {TableNames.samples_lineages} sl
                inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
                where l.{ColumnNames.lineage_name} = :lineage
            )
            order by md5(s.{ColumnNames.accession} || :seed)
            limit :n_samples
        ),
        picked_bm as (
            select coalesce(rb_build_agg(id), rb_build('{{}}')) as bm
            from picked
        )
        select
            '{HOST}' as level,
            {aa_columns},
            ((lower(v.alt_freq_range) + upper(v.alt_freq_range)) / 2.0)::double precision as alt_freq
        from {TableNames.ih_samples_by_amino_acid} v
        inner join {TableNames.amino_acids} c on c.id = v.{ColumnNames.amino_acid_id}
        cross join lateral unnest(
            rb_to_array(v.{ColumnNames.samples_present} & (select bm from picked_bm))
        ) as u({ColumnNames.sample_id})
        inner join picked p on p.id = u.{ColumnNames.sample_id}
        where c.{ColumnNames.gff_feature} = :region

        union all

        select
            '{POPULATION}' as level,
            {aa_columns},
            1.0::double precision as alt_freq
        from {TableNames.cns_samples_by_amino_acid} v
        inner join {TableNames.amino_acids} c on c.id = v.{ColumnNames.amino_acid_id}
        cross join lateral unnest(
            rb_to_array(v.{ColumnNames.samples_present} & (select bm from picked_bm))
        ) as u({ColumnNames.sample_id})
        inner join picked p on p.id = u.{ColumnNames.sample_id}
        where c.{ColumnNames.gff_feature} = :region
    '''
    params = {
        'hosts': hosts,
        'location': location,
        'lineage': lineage,
        'seed': str(seed),
        'n_samples': n_samples,
        'region': region,
    }

    async with get_async_session() as session:
        res = await session.execute(text(query), params)
        ihv_rows, cns_rows = [], []
        for row in res.mappings().all():
            target = ihv_rows if row['level'] == HOST else cns_rows
            target.append(_AaRow(**{k: row[k] for k in _AaRow._fields}))
    return ihv_rows, cns_rows


def _reconstruct_ref_rows(ihv_rows: List[_AaRow]) -> List[_AaRow]:
    """Add a ref -> ref row per (sample, feature, position, ref codon) carrying ref_freq = 1 - sum(alt freqs)."""
    groups: Dict[tuple, List[_AaRow]] = defaultdict(list)
    for r in ihv_rows:
        groups[(r.accession, r.gff_feature, r.position_aa, r.ref_codon)].append(r)

    ref_rows = [
        rows[0]._replace(
            alt_codon=rows[0].ref_codon,
            alt_aa=rows[0].ref_aa,
            alt_freq=1 - sum(r.alt_freq for r in rows),
        )
        for rows in groups.values()
    ]
    return ihv_rows + ref_rows


def _cal_relative_day(
    ihv_rows: List[_AaRow],
    cns_rows: List[_AaRow]
) -> Tuple[List[_AaRow], List[_AaRow], Dict[str, int]]:
    """Map each sample with data to its collection day relative to the earliest sample."""
    date_by_sample = {r.accession: r.collection_date for r in ihv_rows + cns_rows}
    first_date = min(date_by_sample.values())
    day_by_sample = {sra: (d - first_date).days for sra, d in date_by_sample.items()}
    return ihv_rows, cns_rows, day_by_sample


def _call_against_consensus(ihv_rows: List[_AaRow], cns_rows: List[_AaRow]) -> List[_AaRow]:
    """Minor variants are called against the sample's consensus codon, use ref if no consensus codon."""
    cns_codon: Dict[tuple, str] = {}
    for r in cns_rows:
        key = (r.accession, r.gff_feature, r.position_aa)
        if key in cns_codon:
            raise RuntimeError(f'Multiple consensus amino-acid changes for sample/feature/position {key}')
        cns_codon[key] = r.alt_codon

    out = []
    for r in ihv_rows:
        ref_codon = cns_codon.get((r.accession, r.gff_feature, r.position_aa), r.ref_codon)
        ref_aa = CODON_TO_AA.get(ref_codon, 'X')
        if r.alt_codon != ref_codon and ref_aa != 'X' and r.alt_aa != 'X':
            out.append(r._replace(ref_codon=ref_codon, ref_aa=ref_aa))
    return out


def _annotate(r: _AaRow) -> str:
    if r.ref_aa == r.alt_aa:
        return S
    if r.alt_aa == '*':
        return STOP
    return NS


def _cal_divergence_per_site(rows: List[_AaRow], samples: List[str], gff_feature: str) -> Dict[str, List[float]]:
    sample_set = set(samples)
    sums: Dict[str, Dict[str, float]] = {sra: dict.fromkeys(CLASSES, 0.0) for sra in samples}
    for r in rows:
        if r.accession in sample_set:
            sums[r.accession][_annotate(r)] += r.alt_freq

    n_sites = dict(zip(CLASSES, N_SITES[gff_feature]))
    return {cls: [sums[sra][cls] / n_sites[cls] for sra in samples] for cls in CLASSES}
