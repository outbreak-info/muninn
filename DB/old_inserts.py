import asyncio
import json
import sys
from datetime import datetime
from glob import glob
from typing import Type

from sqlalchemy import and_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from DB.engine import engine, get_async_session
from DB.inserts.amino_acid_substitutions import find_or_insert_aa_sub
from DB.models import Sample, Allele, Base, Mutation, AminoAcidSubstitution


async def find_or_add_sample(session: AsyncSession, accession: str) -> Sample:
    sample = await session.scalar(
        select(Sample).where(Sample.accession == accession)
    )
    if sample is None:
        raise NotImplementedError("This scenario is no longer recoverable")
    return sample


def value_or_none(row, key, fn=None):
    try:
        v = row[key]
        if v == '':
            v = None
        elif fn is not None:
            return fn(v)
        return v
    except KeyError:
        return None
    except ValueError:
        return None


async def parse_and_insert_mutations(mutations_files):
    errors = []

    for file in mutations_files:
        with open(file, 'r') as f:
            for row in json.load(f):
                accession = row['sra']
                region = row['region']
                position_nt = int(row['pos'])
                ref_nt = row['ref']
                alt_nt = row['alt']

                async with get_async_session() as session:
                    try:
                        sample = await find_or_add_sample(session, accession)

                        allele = await session.scalar(
                            select(Allele).where(
                                and_(
                                    Allele.region == region,
                                    Allele.position_nt == position_nt,
                                    Allele.alt_nt == alt_nt
                                )
                            )
                        )
                        if allele is None:
                            allele = Allele(
                                position_nt=position_nt,
                                region=region,
                                ref_nt=ref_nt,
                                alt_nt=alt_nt
                            )
                            session.add(allele)
                            await session.commit()
                            await session.refresh(allele)

                        for aa_data in row['aa_mutations']:
                            gff_feature = aa_data['GFF_FEATURE']
                            ref_aa = aa_data['ref_aa']
                            alt_aa = aa_data['alt_aa']
                            position_aa = aa_data['pos_aa']
                            ref_codon = aa_data['ref_codon']
                            alt_codon = aa_data['alt_codon']

                            await find_or_insert_aa_sub(
                                AminoAcidSubstitution(
                                    gff_feature=gff_feature,
                                    ref_aa=ref_aa,
                                    position_aa=position_aa,
                                    alt_aa=alt_aa,
                                    ref_codon=ref_codon,
                                    alt_codon=alt_codon
                                )
                            )

                        mutation = Mutation(sample_id=sample.id, allele_id=allele.id)
                        session.add(mutation)
                        await session.commit()
                    except IntegrityError as e:
                        errors.append(e)
                        continue
                    except NotImplementedError as e:
                        errors.append(e)
                        continue
    print(f'Mutations done with {len(errors)} errors')


def table_has_rows(table: Type['Base']) -> bool:
    with Session(engine) as session:
        return session.query(table).count() > 0


async def main(basedir):
    start = datetime.now()
    print(f'start at {start}')

    if not table_has_rows(Mutation):
        mutations_files = glob(f'{basedir}/mutdata_complete/*.json')
        await parse_and_insert_mutations(mutations_files)
    else:
        print('Mutations already has data, skipping...')

    mutations_done = datetime.now()
    print(f'Mutations took: {mutations_done - start}')

    end = datetime.now()
    print(f'Total elapsed: {end - start}')


if __name__ == '__main__':
    basedir_ = '/home/james/Documents/andersen_lab/bird_flu_db/test_data'
    if len(sys.argv) >= 2:
        basedir_ = sys.argv[1]
    asyncio.run(main(basedir_))
