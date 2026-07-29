import csv
from datetime import datetime
from enum import Enum
from os import path
from typing import Set, List

from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session, get_async_session
from DB.inserts.file_parsers.file_parser import FileParser
from DB.structure import ih_samples_by_allele, ih_samples_by_amino_acid
from DB.structure.constraint_manager import ConstraintManager
from utils.constants import ColumnNames, CONTAINER_DATA_DIRECTORY, Env, ConstraintNames, TableNames, CODONS_AMINO_ACIDS

AMINO_ACID_REF_CONFLICTS_FILE = '/tmp/amino_acid_ref_conflicts.csv'
ALLELE_REF_CONFLICTS_FILE = '/tmp/allele_ref_conflicts.csv'


class RecordType(Enum):
    mutations = 1
    variants = 2
    ih_nts = 3
    ih_codons = 4


class VariantsMutationsCombinedParser(FileParser):

    def __init__(self, filenames: List[str]):
        self.delimiter = '\t'
        # All validation now handled in the InputFile class
        self.input_files = [
            VariantsMutationsCombinedParser.InputFile(name, delimiter=self.delimiter) for name in filenames
            if len(name.strip()) > 0
        ]
        self.n_freq_bins = 20

    async def parse_and_insert(self):
        print(f'{self._get_timestamp()} setup')
        await self._set_up_codon_translation()
        await self._set_up_freq_bins()

        print(f'{self._get_timestamp()} read mutations')
        await self._read_mutations_input()

        print(f'{self._get_timestamp()} read intrahost nt')
        await self._read_ih_nts_input()

        print(f'{self._get_timestamp()} insert alleles')
        await self._stage_alleles()
        await self._write_allele_ref_conflicts()
        await self._drop_fks_using_allele_id()
        await self._drop_alleles_indexes()
        await self._insert_alleles()
        await self._restore_alleles_indexes()

        print(f'{self._get_timestamp()} read intrahost codons')
        await self._read_ih_codons_input()

        print(f'{self._get_timestamp()} insert amino acids')
        await self._stage_amino_acids()
        await self._write_amino_acid_ref_conflicts()
        await self._drop_fks_using_amino_acid_id()
        await self._drop_amino_acids_indexes()
        await self._insert_amino_acids()
        await self._restore_amino_acids_indexes()
        await self._restore_fks_using_amino_acid_id()

        print(f'{self._get_timestamp()} insert intrahost samples - alleles')
        await self._stage_ih_samples_by_allele()
        await self._drop_ih_samples_by_allele_indexes()
        await self._insert_ih_samples_by_allele()
        await self._restore_ih_samples_by_allele_indexes()

        print(f'{self._get_timestamp()} insert consensus samples - alleles')
        await self._stage_cns_samples_by_allele()
        await self._insert_cns_samples_by_allele()
        await self._restore_cns_samples_by_allele_indexes()

        print(f'{self._get_timestamp()} insert intrahost samples - amino acids')
        await self._stage_ih_samples_by_amino_acid()
        await self._drop_ih_samples_by_amino_acid_indexes()
        await self._insert_ih_samples_by_amino_acid()
        await self._restore_ih_samples_by_amino_acid_indexes()

        print(f'{self._get_timestamp()} insert consensus samples - amino acids')
        await self._stage_cns_samples_by_amino_acid()
        await self._insert_cns_samples_by_amino_acid()
        await self._restore_cns_samples_by_amino_acid_indexes()

        print(f'{self._get_timestamp()} clean up tmp tables')
        await self._clean_up_tmp_tables()

        print(f'{self._get_timestamp()} transpose consensus alleles')
        await self._transpose_cns_alleles()

        print(f'{self._get_timestamp()} transpose consensus amino acids')
        await self._transpose_cns_amino_acids()

        print(f'Finished at {self._get_timestamp()}')

    async def _read_mutations_input(self):
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    f'create unlogged table tmp_mutations\n'
                    f'(\n'
                    f'    accession   text      not null,\n'
                    f'    region      text      not null,\n'
                    f'    position_nt int       not null,\n'
                    f'    ref_nt      text,\n'
                    f'    alt_nt      text      not null,\n'
                    f'    gff_feature text,\n'
                    f'    ref_codon   text,\n'
                    f'    alt_codon   text,\n'
                    f'    ref_aa      text,\n'
                    f'    alt_aa      text,\n'
                    f'    position_aa int\n'
                    f');'
                )
            )
            for file in self.input_files:
                if file.record_type == RecordType.mutations:
                    await session.execute(
                        text(
                            f"copy tmp_mutations ({', '.join(file.header_order)})\n"
                            f"from '/muninn/data/{file.relative_name}' delimiter E'{file.delimiter}' csv header;"
                        )
                    )

            await session.execute(
                text(
                    'delete from tmp_mutations\n'
                    'where accession not in (\n'
                    '    select accession\n'
                    '    from samples\n'
                    ');'
                )
            )
            res = await session.execute(
                text('select count(*) from tmp_mutations where ref_nt is null; ')
            )
            count = res.mappings().one()['count']
            if count > 0:
                print(f'Warning: {count} mutations had null ref_nt and will be ignored')

            await session.execute(
                text('delete from tmp_mutations where ref_nt is null;')
            )

            await session.execute(text('create index ix_tmp_mutations_accession on tmp_mutations (accession);'))

            await session.commit()

    async def _read_ih_nts_input(self):
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'create unlogged table tmp_ih_nt (\n'
                    '    region      text    not null,\n'
                    '    position_nt int     not null,\n'
                    '    ref_nt      text    not null,\n'
                    '    alt_nt      text    not null,\n'
                    '    ref_dp      int,\n'
                    '    ref_rv      int,\n'
                    '    ref_qual    float,\n'
                    '    alt_dp      int,\n'
                    '    alt_rv      int,\n'
                    '    alt_qual    float,\n'
                    '    alt_freq    float   not null,\n'
                    '    total_dp    int,\n'
                    '    pval        float,\n'
                    '    pass_qc     bool,\n'
                    '    gff_feature text,\n'
                    '    ref_codon   text,\n'
                    '    ref_aa      text,\n'
                    '    alt_codon   text,\n'
                    '    alt_aa      text,\n'
                    '    position_aa int,\n'
                    '    gapped_freq numeric not null,\n'
                    '    gapped_dp   int,\n'
                    '    flagged_pos bool,\n'
                    '    amp_masked  bool,\n'
                    '    std_dev     float,\n'
                    '    amp_freq    float,\n'
                    '    amp_numbers int,\n'
                    '    accession   text    not null\n'
                    ');'
                )
            )

            for file in self.input_files:
                if file.record_type == RecordType.ih_nts:
                    await session.execute(
                        text(
                            f"copy tmp_ih_nt ({', '.join(file.header_order)})\n"
                            f"from '/muninn/data/{file.relative_name}' delimiter E'{file.delimiter}' csv header;"
                        )
                    )

            await session.execute(text('delete from tmp_ih_nt where alt_nt = ref_nt;'))
            # todo: any more filtering? depth?
            await session.execute(
                text(
                    'delete from tmp_ih_nt\n'
                    'where accession not in (\n'
                    '    select accession\n'
                    '    from samples\n'
                    ');'
                )
            )
            await session.execute(text('create index ix_tmp_ih_nt_accession on tmp_ih_nt (accession);'))
            await session.commit()

    async def _read_ih_codons_input(self):
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'create unlogged table tmp_ih_codons (\n'
                    '    gff_feature text  not null,\n'
                    '    position_aa int   not null,\n'
                    '    ref_codon   text  not null,\n'
                    '    alt_codon   text  not null,\n'
                    '    ref_dp      int   not null,\n'
                    '    alt_dp      int   not null,\n'
                    '    alt_freq    float not null,\n'
                    '    position_nt text,\n'
                    '    ref_nt      text,\n'
                    '    alt_nt      text,\n'
                    '    accession   text  not null\n'
                    ');'
                )
            )
            for file in self.input_files:
                if file.record_type == RecordType.ih_codons:
                    await file.copy_into_table('tmp_ih_codons', session)  # todo: if this works use it all over

            await session.execute(text('delete from tmp_ih_codons where alt_codon = ref_codon;'))
            # todo: any more filtering? depth?
            await session.execute(
                text(
                    'delete\n'
                    'from tmp_ih_codons\n'
                    'where accession not in (\n'
                    '    select accession\n'
                    '    from samples\n'
                    ');'
                )
            )
            await session.execute(text('create index ix_tmp_ih_codons_accession on tmp_ih_codons (accession);'))
            await session.commit()

    @staticmethod
    async def _stage_alleles():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'create unlogged table tmp_alleles\n'
                    '(\n'
                    '    region      text not null,\n'
                    '    position_nt int  not null,\n'
                    '    ref_nt      text not null,\n'
                    '    alt_nt      text not null\n'
                    ');'
                )
            )

            await session.execute(
                text('create index ix_tmp_mutations_nt_values on tmp_mutations (region, position_nt, ref_nt, alt_nt);')
            )
            await session.execute(
                text('create index ix_tmp_ih_nt_nt_values on tmp_ih_nt (region, position_nt, ref_nt, alt_nt);')
            )

            # get allele values from mutations
            await session.execute(
                text(
                    'insert into tmp_alleles (\n'
                    '    region, position_nt, ref_nt, alt_nt\n'
                    ')\n'
                    'select region, position_nt, ref_nt, alt_nt\n'
                    'from tmp_mutations\n'
                    'group by region, position_nt, ref_nt, alt_nt;'
                )
            )
            # get allele values from intrahost
            # Skipping over entries already in tmp_alleles
            await session.execute(
                text(
                    'insert into tmp_alleles (\n'
                    '    region, position_nt, ref_nt, alt_nt\n'
                    ')\n'
                    'select region, position_nt, ref_nt, alt_nt\n'
                    'from tmp_ih_nt\n'
                    'where (region, position_nt, ref_nt, alt_nt) not in (\n'
                    '    select region, position_nt, ref_nt, alt_nt\n'
                    '    from tmp_alleles\n'
                    ')\n'
                    'group by region, position_nt, ref_nt, alt_nt;'
                )
            )

            await session.execute(
                text('create index ix_tmp_alleles on tmp_alleles (region, position_nt, alt_nt, ref_nt);')
            )

            # delete existing alleles from tmp_alleles, it's important to ignore ref_nt here
            await session.execute(
                text(
                    'delete\n'
                    'from tmp_alleles\n'
                    'where (region, position_nt, alt_nt) in (\n'
                    '    select region, position_nt, alt_nt\n'
                    '    from alleles\n'
                    ');'
                )
            )

            await session.commit()

    @staticmethod
    async def _insert_alleles():
        async with get_async_write_session() as session:
            # insert, takes the first value in ref conflicts
            await session.execute(
                text(
                    'insert into alleles (\n'
                    '    region, position_nt, alt_nt, ref_nt\n'
                    ')\n'
                    'select distinct on (region, position_nt, alt_nt) region, position_nt, alt_nt, ref_nt\n'
                    'from tmp_alleles;'
                )
            )

            await session.commit()

    @staticmethod
    async def _write_allele_ref_conflicts():
        async with get_async_session() as session:
            res = await session.execute(
                text(
                    'select region,\n'
                    '       position_nt,\n'
                    '       alt_nt,\n'
                    '       array_agg(distinct ref_nt) as ref_nts\n'
                    'from tmp_alleles\n'
                    'group by region, position_nt, alt_nt\n'
                    'having cardinality(array_agg(distinct ref_nt)) > 1;'
                )
            )
        conflicts = res.mappings().all()
        with open(ALLELE_REF_CONFLICTS_FILE, 'w+') as f:
            if len(conflicts) > 0:
                print(f'Warning: {len(conflicts)} allele ref conflicts found. See {ALLELE_REF_CONFLICTS_FILE}')
                writer = csv.DictWriter(f, fieldnames=conflicts[0].keys())
                writer.writeheader()
                writer.writerows(conflicts)
            else:
                print('no conflicts found', file=f)

    @staticmethod
    async def _stage_amino_acids():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'create unlogged table tmp_amino_acids\n'
                    '(\n'
                    '    gff_feature text not null,\n'
                    '    ref_aa      text not null,\n'
                    '    alt_aa      text not null,\n'
                    '    position_aa int  not null,\n'
                    '    ref_codon   text not null,\n'
                    '    alt_codon   text not null\n'
                    ');'
                )
            )

            await session.execute(
                text(
                    'create index ix_tmp_mutations_aa_values on tmp_mutations (gff_feature, position_aa, alt_aa, alt_codon, ref_aa, ref_codon);'
                )
            )
            await session.execute(
                text(
                    'create index ix_tmp_ih_codons_aa_values on tmp_ih_codons (gff_feature, position_aa, alt_codon, ref_codon);'
                )
            )

            # from intrahost codons
            await session.execute(
                text(
                    'insert into tmp_amino_acids (\n'
                    '    gff_feature, position_aa, alt_aa, alt_codon, ref_aa, ref_codon\n'
                    ')\n'
                    'select distinct on (gff_feature, position_aa, alt_codon, ref_codon)\n'
                    '       gff_feature,\n'
                    '       position_aa,\n'
                    '       translate_codon(alt_codon) as alt_aa,\n'
                    '       alt_codon,\n'
                    '       translate_codon(ref_codon) as ref_aa,\n'
                    '       ref_codon\n'
                    'from tmp_ih_codons;\n'
                )
            )

            # get amino acid values from mutations
            await session.execute(
                text(
                    'insert into tmp_amino_acids (\n'
                    '    gff_feature, position_aa, alt_aa, alt_codon, ref_aa, ref_codon\n'
                    ')\n'
                    'select gff_feature, position_aa, alt_aa, alt_codon, ref_aa, ref_codon\n'
                    'from tmp_mutations\n'
                    'where gff_feature is not null\n'
                    'and position_aa is not null\n'
                    'and alt_aa is not null\n'
                    'and ref_aa is not null\n'
                    'and alt_codon is not null\n'
                    'and ref_codon is not null\n'
                    'group by gff_feature, position_aa, alt_aa, alt_codon, ref_aa, ref_codon;'
                )
            )

            # get values from variants, skipping over values already in tmp_amino_acids
            await session.execute(
                text(
                    'insert into tmp_amino_acids (\n'
                    '    gff_feature, position_aa, alt_aa, alt_codon, ref_aa, ref_codon\n'
                    ')\n'
                    'select distinct on (gff_feature, position_aa, alt_aa, ref_aa, alt_codon, ref_codon)\n'
                    '       gff_feature,\n'
                    '       position_aa,\n'
                    '       alt_aa,\n'
                    '       alt_codon,\n'
                    '       ref_aa,\n'
                    '       ref_codon\n'
                    'from tmp_mutations\n'
                    'where gff_feature is not null\n'
                    '  and position_aa is not null\n'
                    '  and alt_aa is not null\n'
                    '  and ref_aa is not null\n'
                    '  and alt_codon is not null\n'
                    '  and ref_codon is not null\n'
                    '  and (gff_feature, position_aa, alt_aa, alt_codon, ref_aa, ref_codon) not in (\n'
                    '    select gff_feature, position_aa, alt_aa, alt_codon, ref_aa, ref_codon\n'
                    '    from tmp_amino_acids\n'
                    ');'
                )
            )

            await session.execute(
                text(
                    'create index ix_tmp_amino_acids on tmp_amino_acids (gff_feature, position_aa, alt_aa, alt_codon, ref_aa, ref_codon);'
                )
            )
            # delete pre-existing values. Note: it's important to ignore ref aa and ref codon for this
            await session.execute(
                text(
                    'delete\n'
                    'from tmp_amino_acids\n'
                    'where (gff_feature, position_aa, alt_aa, alt_codon) in (\n'
                    '    select gff_feature, position_aa, alt_aa, alt_codon\n'
                    '    from amino_acids\n'
                    ');'
                )
            )
            await session.commit()

    @staticmethod
    async def _insert_amino_acids():
        async with get_async_write_session() as session:
            # insert, takes the first value seen in ref conflicts
            await session.execute(
                text(
                    'insert into amino_acids (\n'
                    '    gff_feature, position_aa, alt_aa, alt_codon, ref_aa, ref_codon\n'
                    ')\n'
                    'select distinct on (gff_feature, position_aa, alt_aa, alt_codon)\n'
                    '       gff_feature,\n'
                    '       position_aa,\n'
                    '       alt_aa,\n'
                    '       alt_codon,\n'
                    '       ref_aa,\n'
                    '       ref_codon\n'
                    'from tmp_amino_acids;'
                )
            )

            await session.commit()

    @staticmethod
    async def _write_amino_acid_ref_conflicts():
        async with get_async_session() as session:
            res = await session.execute(
                text(
                    'with dup_codon as (\n'
                    '    select gff_feature,\n'
                    '           position_aa,\n'
                    '           alt_aa,\n'
                    '           alt_codon,\n'
                    '           array_agg(distinct ref_codon) as ref_codons\n'
                    '    from tmp_amino_acids\n'
                    '    group by gff_feature, position_aa, alt_aa, alt_codon\n'
                    '    having cardinality(array_agg(distinct ref_codon)) > 1\n'
                    '),\n'
                    '     dup_aa as (\n'
                    '    select gff_feature,\n'
                    '           position_aa,\n'
                    '           alt_aa,\n'
                    '           alt_codon,\n'
                    '           array_agg(distinct ref_aa) as ref_aas\n'
                    '    from tmp_amino_acids\n'
                    '    group by gff_feature, position_aa, alt_aa, alt_codon\n'
                    '    having cardinality(array_agg(distinct ref_aa)) > 1\n'
                    ')\n'
                    'select *\n'
                    'from dup_codon dc\n'
                    'full join dup_aa daa using (gff_feature, position_aa, alt_aa, alt_codon);'
                )
            )
        conflicts = res.mappings().all()
        with open(AMINO_ACID_REF_CONFLICTS_FILE, 'w+') as f:
            if len(conflicts) > 0:
                print(f'Warning: {len(conflicts)} amino acid ref conflicts found. See {AMINO_ACID_REF_CONFLICTS_FILE}')
                writer = csv.DictWriter(f, fieldnames=conflicts[0].keys())
                writer.writeheader()
                writer.writerows(conflicts)
            else:
                print('no conflicts found', file=f)

    @staticmethod
    async def _stage_cns_samples_by_allele():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'create unlogged table tmp_mutations_staging as\n'
                    'select a.id as allele_id,\n'
                    '       rb_build_agg(s.id) as s_present\n'
                    'from tmp_mutations tmut\n'
                    'inner join alleles a on a.region = tmut.region and a.position_nt = tmut.position_nt and a.alt_nt = tmut.alt_nt\n'
                    'inner join samples s on s.accession = tmut.accession\n'
                    'group by a.id;'
                )
            )
            await session.commit()

    @staticmethod
    async def _insert_cns_samples_by_allele():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    f'insert into {TableNames.cns_samples_by_allele} as target ({ColumnNames.allele_id}, {ColumnNames.samples_present})\n'
                    f'select allele_id, s_present\n'
                    f'from tmp_mutations_staging\n'
                    f'on conflict on constraint {ConstraintNames.pk_cns_samples_by_allele}\n'
                    f'do update set {ColumnNames.samples_present} = target.{ColumnNames.samples_present} | excluded.{ColumnNames.samples_present};'
                )
            )
            await session.commit()

    async def _set_up_freq_bins(self):
        if 1000 % self.n_freq_bins != 0:
            raise ValueError(f'Invalid number of bins: {self.n_freq_bins}. We require 1000 % nbins == 0.')
        width = int(1000 / self.n_freq_bins)
        breaks = [i / 1000 for i in range(0, 1000, width)]

        bin_ranges = []
        # all but the last range, all half-open
        for i in range(len(breaks)):
            start = breaks[i]
            try:
                end = breaks[i + 1]
            except IndexError:
                break
            bin_ranges.append(f'(numrange({start}, {end}))')
        # add the last bin ending at 1 and closed
        bin_ranges.append(f"(numrange({breaks[-1]}, 1, '[]'))")

        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'create unlogged table tmp_freq_bins (\n'
                    '    bin_range numrange not null\n'
                    ');'
                )
            )
            await session.execute(
                text(f'insert into tmp_freq_bins (bin_range) values {",".join(bin_ranges)};')
            )

            await session.commit()

    @staticmethod
    async def _stage_ih_samples_by_allele():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'create unlogged table tmp_ih_samples_alleles_staging as\n'
                    'select a.id as allele_id, bin.bin_range as freq_bin, rb_build_agg(s.id) as s_present\n'
                    'from tmp_ih_nt tin\n'
                    'left join samples s using (accession)\n'
                    'left join alleles a using (region, position_nt, alt_nt, ref_nt)\n'
                    'left join tmp_freq_bins bin on bin.bin_range @> tin.gapped_freq::numeric\n'
                    'group by a.id, bin.bin_range;'
                )
            )
            await session.commit()

    @staticmethod
    async def _insert_ih_samples_by_allele():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'insert into ih_samples_by_allele as target (allele_id, alt_freq_range, samples_present)\n'
                    'select allele_id, freq_bin, s_present\n'
                    'from tmp_ih_samples_alleles_staging\n'
                    'on conflict on constraint pk_ih_samples_by_allele do update\n'
                    '    set samples_present = target.samples_present | excluded.samples_present;'
                )
            )
            await session.commit()

    @staticmethod
    async def _stage_cns_samples_by_amino_acid():
        async with get_async_write_session() as session:
            # create a partial index on tmp_mutations to help with distinct in the next step
            await session.execute(
                text(
                    'create index ix_tmp_mutations_sample_aa\n'
                    '    on tmp_mutations (accession, gff_feature, position_aa, alt_aa, alt_codon)\n'
                    '    where gff_feature is not null\n'
                    '        and position_aa is not null\n'
                    '        and alt_aa is not null\n'
                    '        and ref_aa is not null;'
                )
            )

            # create staging table
            await session.execute(
                text(
                    'create unlogged table tmp_mutation_translations_staging as\n'
                    'select aa.id as amino_acid_id,\n'
                    '       rb_build_agg(s.id) as s_present\n'
                    'from tmp_mutations tmut\n'
                    'inner join amino_acids aa on\n'
                    '        aa.gff_feature = tmut.gff_feature\n'
                    '            and aa.position_aa = tmut.position_aa\n'
                    '            and aa.alt_aa = tmut.alt_aa\n'
                    '            and aa.alt_codon = tmut.alt_codon\n'
                    'inner join samples s on s.accession = tmut.accession\n'
                    'where tmut.gff_feature is not null\n'
                    '  and tmut.position_aa is not null\n'
                    '  and tmut.alt_aa is not null\n'
                    '  and tmut.ref_aa is not null\n'
                    'group by aa.id;'
                )
            )

            await session.commit()

    @staticmethod
    async def _insert_cns_samples_by_amino_acid():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    f'insert into {TableNames.cns_samples_by_amino_acid} as target (amino_acid_id, {ColumnNames.samples_present})\n'
                    f'select amino_acid_id, s_present\n'
                    f'from tmp_mutation_translations_staging\n'
                    f'on conflict on constraint {ConstraintNames.pk_cns_samples_by_amino_acid}\n'
                    f'    do update set {ColumnNames.samples_present} = target.{ColumnNames.samples_present} | excluded.{ColumnNames.samples_present};'
                )
            )
            await session.commit()

    @staticmethod
    async def _stage_ih_samples_by_amino_acid():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'create unlogged table tmp_ih_samples_by_amino_acid_staging as (\n'
                    '    select aa.id as amino_acid_id,\n'
                    '           bin_range as alt_freq_bin,\n'
                    '           rb_build_agg(s.id) as s_present\n'
                    '    from tmp_ih_codons tic\n'
                    '    left join tmp_freq_bins bins on bins.bin_range @> tic.alt_freq::numeric\n'
                    '    left join amino_acids aa on aa.gff_feature = tic.gff_feature\n'
                    '            and aa.position_aa = tic.position_aa\n'
                    '            and aa.alt_codon = tic.alt_codon\n'
                    '    left join samples s on s.accession = tic.accession\n'
                    '    group by aa.id, bin_range\n'
                    ');'
                )
            )
            await session.commit()

    @staticmethod
    async def _insert_ih_samples_by_amino_acid():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'insert into ih_samples_by_amino_acid as target (amino_acid_id, alt_freq_range, samples_present)\n'
                    'select amino_acid_id, alt_freq_bin, s_present\n'
                    'from tmp_ih_samples_by_amino_acid_staging\n'
                    'on conflict on constraint pk_ih_samples_by_amino_acid\n'
                    '    do update set samples_present = target.samples_present | excluded.samples_present;'
                )
            )
            await session.commit()

    @staticmethod
    async def _transpose_cns_alleles():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    f'insert into {TableNames.cns_alleles_by_sample} as target ({ColumnNames.sample_id}, {ColumnNames.alleles_present}) (\n'
                    f'    with pairs as (\n'
                    f'    select {ColumnNames.allele_id}, unnest(rb_to_array({ColumnNames.samples_present})) as {ColumnNames.sample_id} from {TableNames.cns_samples_by_allele}\n'
                    f'    )\n'
                    f'    select {ColumnNames.sample_id}, rb_build_agg({ColumnNames.allele_id})\n'
                    f'    from pairs\n'
                    f'    group by {ColumnNames.sample_id}\n'
                    f')\n'
                    f'on conflict on constraint {ConstraintNames.pk_cns_alleles_by_sample} do update\n'
                    f'set {ColumnNames.alleles_present} = target.{ColumnNames.alleles_present} | excluded.{ColumnNames.alleles_present};'
                )
            )
            await session.commit()

    @staticmethod
    async def _transpose_cns_amino_acids():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    f'insert into {TableNames.cns_amino_acids_by_sample} as target ({ColumnNames.sample_id}, {ColumnNames.amino_acids_present}) (\n'
                    f'    with pairs as (\n'
                    f'    select {ColumnNames.amino_acid_id}, unnest(rb_to_array({ColumnNames.samples_present})) as {ColumnNames.sample_id} from {TableNames.cns_samples_by_amino_acid}\n'
                    f'    )\n'
                    f'    select {ColumnNames.sample_id}, rb_build_agg({ColumnNames.amino_acid_id})\n'
                    f'    from pairs\n'
                    f'    group by {ColumnNames.sample_id}\n'
                    f')\n'
                    f'on conflict on constraint {ConstraintNames.pk_cns_amino_acids_by_sample} do update\n'
                    f'set {ColumnNames.amino_acids_present} = target.{ColumnNames.amino_acids_present} | excluded.{ColumnNames.amino_acids_present};'
                )
            )
            await session.commit()

    @staticmethod
    async def _set_up_codon_translation():

        async with get_async_write_session() as session:
            await session.execute(
                text(
                    'create unlogged table tmp_codon_translations (\n'
                    '    codon text not null unique,\n'
                    '    aa    text not null\n'
                    ');'
                )
            )
            await session.execute(
                text(
                    f"insert into tmp_codon_translations (codon, aa)\n"
                    f"values {','.join(str(t) for t in CODONS_AMINO_ACIDS)};"
                )
            )
            await session.execute(
                text(
                    "create or replace function translate_codon(codon_in text)\n"
                    "    returns text as\n"
                    "$$\n"
                    "declare\n"
                    "    aa_out text;\n"
                    "begin\n"
                    "    if codon_in ~ '.*N.*' then\n"
                    "        select 'X' into aa_out;\n"
                    "    else\n"
                    "        select aa\n"
                    "        from tmp_codon_translations\n"
                    "        where codon = codon_in\n"
                    "        into aa_out;\n"
                    "    end if;\n"
                    "    return aa_out;\n"
                    "end;\n"
                    "$$\n"
                    "    language plpgsql;"
                )
            )
            await session.commit()

    @staticmethod
    async def _clean_up_tmp_tables():
        temp_tables = [
            'tmp_mutations',
            'tmp_ih_nt',
            'tmp_ih_codons',
            'tmp_alleles',
            'tmp_amino_acids',
            'tmp_mutations_staging',
            'tmp_mutation_translations_staging',
            'tmp_ih_samples_alleles_staging',
            'tmp_cns_samples_by_amino_acid_staging',
            'tmp_ih_samples_by_amino_acid_staging',
            'tmp_intra_host_translations_staging',
            'tmp_codon_translations',
            'tmp_freq_bins',
        ]
        async with get_async_write_session() as session:
            for t in temp_tables:
                await session.execute(
                    text(f'drop table if exists {t};')
                )

            await session.execute(
                text(
                    'drop function if exists translate_codon(codon_in text);'
                )
            )

            await session.commit()

    def _get_header_order(self, filename, column_name_mapping):
        proper_col_names = {
            v: k for k, v in column_name_mapping.items()
        }
        ordered_header = []
        with open(filename, 'r') as f:
            header = f.readline().split(self.delimiter)
            ordered_header = [proper_col_names[h.strip()] for h in header]
        if len(ordered_header) != len(proper_col_names.keys()):
            raise ValueError('mutations header bad')
        return ordered_header

    @staticmethod
    def _find_relative_and_local_abs_paths(filename: str) -> tuple[str, str]:
        """
        Find absolute and relative paths for given filename
        either within container's bound data directory (if running in a container)
        or within the bound directory on the host machine (if running outside container).
        Raise ValueError if filename not found in either place.
        :param filename: input filename
        :return: (relative path, absolute local path)
        """
        if path.isabs(filename):
            # if we're given an abs path, it must point to one of these locations.
            for data_dir in [CONTAINER_DATA_DIRECTORY, Env.MUNINN_SERVER_DATA_INPUT_DIR]:
                if path.commonprefix([filename, data_dir]) == data_dir:
                    if path.isfile(filename):
                        return path.relpath(filename, data_dir), filename
        else:
            # we have a relative path. try to find the file in each valid dir.
            for data_dir in [CONTAINER_DATA_DIRECTORY, Env.MUNINN_SERVER_DATA_INPUT_DIR]:
                putative_abs = path.join(data_dir, filename)
                if path.isfile(putative_abs):
                    return filename, putative_abs

        raise ValueError(
            f'{filename} not found within {CONTAINER_DATA_DIRECTORY} or {Env.MUNINN_SERVER_DATA_INPUT_DIR}. '
            f'The file must be in the bound data directory.'
        )

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return {
            f' variants: {", ".join(VariantsMutationsCombinedParser.variants_column_mapping.values())}',
            f'mutations: {", ".join(VariantsMutationsCombinedParser.mutations_column_mapping.values())}'
        }

    @staticmethod
    def _get_timestamp():
        return datetime.now().isoformat(timespec='seconds')

    @staticmethod
    async def _drop_fks_using_allele_id():
        await ConstraintManager.drop_constraints(
            [
                ConstraintNames.fk_cns_samples_by_allele_allele_id_alleles,
                ConstraintNames.fk_ih_samples_by_allele_allele_id_alleles,
            ]
        )

    @staticmethod
    async def _drop_alleles_indexes():
        await ConstraintManager.drop_constraints(
            [
                ConstraintNames.uq_alleles_nt_values,
                ConstraintNames.pk_alleles
            ]
        )

    @staticmethod
    async def _restore_alleles_indexes():
        await ConstraintManager.restore_constraints(
            [
                ConstraintNames.pk_alleles,
                ConstraintNames.uq_alleles_nt_values,
            ]
        )

    @staticmethod
    async def _drop_fks_using_amino_acid_id():
        await ConstraintManager.drop_constraints(
            [
                ConstraintNames.fk_cns_samples_by_amino_acid_amino_acid_id_amino_acids,
                ConstraintNames.fk_phenotype_metric_values_amino_acid_id_amino_acids,
                ConstraintNames.fk_annotations_amino_acids_amino_acid_id_amino_acids,
                ConstraintNames.fk_ih_samples_by_amino_acid_amino_acid_id_amino_acids,
            ]
        )

    @staticmethod
    async def _drop_amino_acids_indexes():
        await ConstraintManager.drop_constraints(
            [
                ConstraintNames.uq_amino_acids_gff_feature_position_alt_aa_alt_codon,
                ConstraintNames.pk_amino_acids,
            ]
        )

    @staticmethod
    async def _restore_amino_acids_indexes():
        await ConstraintManager.restore_constraints(
            [
                ConstraintNames.pk_amino_acids,
                ConstraintNames.uq_amino_acids_gff_feature_position_alt_aa_alt_codon
            ]
        )

    @staticmethod
    async def _restore_fks_using_amino_acid_id():
        await ConstraintManager.restore_constraints(
            [
                ConstraintNames.fk_phenotype_metric_values_amino_acid_id_amino_acids,
                ConstraintNames.fk_annotations_amino_acids_amino_acid_id_amino_acids,
            ]
        )

    @staticmethod
    async def _drop_ih_samples_by_allele_indexes():
        await ih_samples_by_allele.drop_trigger_check_range_overlap()

    @staticmethod
    async def _restore_ih_samples_by_allele_indexes():
        await ConstraintManager.restore_constraints(
            [
                ConstraintNames.fk_ih_samples_by_allele_allele_id_alleles,
            ]
        )
        await ih_samples_by_allele.restore_trigger_check_range_overlap()

    @staticmethod
    async def _restore_cns_samples_by_allele_indexes():
        await ConstraintManager.restore_constraints(
            [
                ConstraintNames.fk_cns_samples_by_allele_allele_id_alleles,
            ]
        )

    @staticmethod
    async def _drop_ih_samples_by_amino_acid_indexes():
        await ih_samples_by_amino_acid.drop_trigger_check_range_overlap()

    @staticmethod
    async def _restore_ih_samples_by_amino_acid_indexes():
        await ConstraintManager.restore_constraints(
            [
                ConstraintNames.fk_ih_samples_by_amino_acid_amino_acid_id_amino_acids,
            ]
        )
        await ih_samples_by_amino_acid.restore_trigger_check_range_overlap()

    @staticmethod
    async def _restore_cns_samples_by_amino_acid_indexes():
        await ConstraintManager.restore_constraints(
            [
                ConstraintNames.fk_cns_samples_by_amino_acid_amino_acid_id_amino_acids,
            ]
        )

    variants_column_mapping = {
        ColumnNames.region: 'REGION',
        ColumnNames.position_nt: 'POS',
        ColumnNames.ref_nt: 'REF',
        ColumnNames.alt_nt: 'ALT',
        ColumnNames.position_aa: 'POS_AA',
        ColumnNames.ref_aa: 'REF_AA',
        ColumnNames.alt_aa: 'ALT_AA',
        ColumnNames.gff_feature: 'GFF_FEATURE',
        ColumnNames.ref_codon: 'REF_CODON',
        ColumnNames.alt_codon: 'ALT_CODON',
        ColumnNames.accession: 'SRA',
        ColumnNames.pval: 'PVAL',
        ColumnNames.ref_dp: 'REF_DP',
        ColumnNames.ref_rv: 'REF_RV',
        ColumnNames.ref_qual: 'REF_QUAL',
        ColumnNames.alt_dp: 'ALT_DP',
        ColumnNames.alt_rv: 'ALT_RV',
        ColumnNames.alt_qual: 'ALT_QUAL',
        ColumnNames.pass_qc: 'PASS',
        ColumnNames.alt_freq: 'ALT_FREQ',
        ColumnNames.total_dp: 'TOTAL_DP',
    }

    mutations_column_mapping = {
        ColumnNames.accession: 'sra',
        ColumnNames.position_nt: 'pos',
        ColumnNames.ref_nt: 'ref',
        ColumnNames.alt_nt: 'alt',
        ColumnNames.region: 'region',
        ColumnNames.gff_feature: 'GFF_FEATURE',
        ColumnNames.ref_codon: 'ref_codon',
        ColumnNames.alt_codon: 'alt_codon',
        ColumnNames.ref_aa: 'ref_aa',
        ColumnNames.alt_aa: 'alt_aa',
        ColumnNames.position_aa: 'pos_aa',
    }

    intrahost_nts_column_mapping = {
        ColumnNames.region: 'REGION',
        ColumnNames.position_nt: 'POS',
        ColumnNames.ref_nt: 'REF',
        ColumnNames.alt_nt: 'ALT',
        ColumnNames.ref_dp: 'REF_DP',
        ColumnNames.ref_rv: 'REF_RV',
        ColumnNames.ref_qual: 'REF_QUAL',
        ColumnNames.alt_dp: 'ALT_DP',
        ColumnNames.alt_rv: 'ALT_RV',
        ColumnNames.alt_qual: 'ALT_QUAL',
        ColumnNames.alt_freq: 'ALT_FREQ',
        ColumnNames.total_dp: 'TOTAL_DP',
        ColumnNames.pval: 'PVAL',
        ColumnNames.pass_qc: 'PASS',
        ColumnNames.gff_feature: 'GFF_FEATURE',
        ColumnNames.ref_codon: 'REF_CODON',
        ColumnNames.ref_aa: 'REF_AA',
        ColumnNames.alt_codon: 'ALT_CODON',
        ColumnNames.alt_aa: 'ALT_AA',
        ColumnNames.position_aa: 'POS_AA',
        ColumnNames.gapped_freq: 'GAPPED_FREQ',
        'gapped_dp': 'GAPPED_DEPTH',
        'flagged_pos': 'FLAGGED_POS',
        'amp_masked': 'AMP_MASKED',
        'std_dev': 'STD_DEV',
        'amp_freq': 'AMP_FREQ',
        'amp_numbers': 'AMP_NUMBERS',
        ColumnNames.accession: 'SRA',
    }

    intrahost_codons_column_mapping = {
        ColumnNames.gff_feature: 'GFF_FEATURE',
        ColumnNames.position_aa: 'POS_CODON',
        ColumnNames.ref_codon: 'REF_CODON',
        ColumnNames.alt_codon: 'ALT_CODON',
        ColumnNames.ref_dp: 'REF_DEPTH_CODON',
        ColumnNames.alt_dp: 'ALT_DEPTH_CODON',
        ColumnNames.alt_freq: 'ALT_FREQ_CODON',
        ColumnNames.position_nt: 'POS',
        ColumnNames.ref_nt: 'REF',
        ColumnNames.alt_nt: 'ALT',
        ColumnNames.accession: 'SRA',

    }

    class InputFile:
        def __init__(self, filename: str, delimiter: str = '\t'):
            self.delimiter = delimiter
            self.raw_name = filename
            self.relative_name, self.local_name = (
                VariantsMutationsCombinedParser._find_relative_and_local_abs_paths(self.raw_name)
            )
            self.record_type: RecordType = self._choose_record_type()
            self.header_order: List[str] = self._get_header_order()

        def _choose_record_type(self):
            variants_columns = set(VariantsMutationsCombinedParser.variants_column_mapping.values())
            mutations_columns = set(VariantsMutationsCombinedParser.mutations_column_mapping.values())
            intrahost_nts_columns = set(VariantsMutationsCombinedParser.intrahost_nts_column_mapping.values())
            intrahost_codons_columns = set(VariantsMutationsCombinedParser.intrahost_codons_column_mapping.values())

            with open(self.local_name, 'r') as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                fieldnames = set(reader.fieldnames)

                if fieldnames == intrahost_nts_columns:
                    return RecordType.ih_nts
                elif fieldnames == intrahost_codons_columns:
                    return RecordType.ih_codons
                elif fieldnames == mutations_columns:
                    return RecordType.mutations
                elif fieldnames == variants_columns:
                    return RecordType.variants
                else:
                    raise ValueError(f'File has an unacceptable header and cannot be processed: {self.raw_name}')

        def _get_header_order(self) -> List[str]:
            column_name_mapping = None
            match self.record_type:
                case RecordType.variants:
                    column_name_mapping = VariantsMutationsCombinedParser.variants_column_mapping
                case RecordType.mutations:
                    column_name_mapping = VariantsMutationsCombinedParser.mutations_column_mapping
                case RecordType.ih_nts:
                    column_name_mapping = VariantsMutationsCombinedParser.intrahost_nts_column_mapping
                case RecordType.ih_codons:
                    column_name_mapping = VariantsMutationsCombinedParser.intrahost_codons_column_mapping

            proper_col_names = {
                v: k for k, v in column_name_mapping.items()
            }
            ordered_header = []
            with open(self.local_name, 'r') as f:
                header = f.readline().split(self.delimiter)
                ordered_header = [proper_col_names[h.strip()] for h in header]
            if len(ordered_header) != len(proper_col_names.keys()):
                raise ValueError(f'Failed to construct header ordering for file: {self.raw_name}')
            return ordered_header

        async def copy_into_table(self, tablename: str, session: AsyncSession):
            await session.execute(
                text(
                    f"copy {tablename} ({', '.join(self.header_order)})\n"
                    f"from '/muninn/data/{self.relative_name}' delimiter E'{self.delimiter}' csv header;"
                )
            )


class VariantsMutationsCombinedParserBig(VariantsMutationsCombinedParser):
    def __init__(self, filenames: List[str]):
        super().__init__(filenames)
        self.tmp_wal_size_mb = 1024 * 20
        self.tmp_checkpoint_timeout_s = 3600

    async def parse_and_insert(self):
        print(
            f'Setting max_wal_size to {self.tmp_wal_size_mb}MB '
            f'and checkpoint_timeout to {self.tmp_checkpoint_timeout_s} s'
        )
        await self._increase_wal_size()
        await super().parse_and_insert()
        print('Resetting max_wal_size and checkpoint_timeout')
        await self._reset_wal_size()

    async def _increase_wal_size(self):
        async with get_async_write_session() as session:
            connection = await session.connection()
            await connection.execute(text('COMMIT'))
            await connection.execute(
                text(f'alter system set max_wal_size = {self.tmp_wal_size_mb}')
            )
            await connection.execute(
                text(f'alter system set checkpoint_timeout = {self.tmp_checkpoint_timeout_s}')
            )
            await connection.execute(
                text('select * from pg_reload_conf()')
            )

    @staticmethod
    async def _reset_wal_size():
        async with get_async_write_session() as session:
            connection = await session.connection()
            await connection.execute(text('COMMIT'))
            await connection.execute(
                text('alter system reset max_wal_size')
            )
            await connection.execute(
                text('alter system reset checkpoint_timeout')
            )
            await connection.execute(
                text('select * from pg_reload_conf()')
            )
