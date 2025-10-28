import csv
from datetime import datetime
from os import path
from typing import Set

from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session, get_async_session
from DB.inserts.file_parsers.file_parser import FileParser
from utils.constants import StandardColumnNames, CONTAINER_DATA_DIRECTORY, Env

AMINO_ACID_REF_CONFLICTS_FILE = '/tmp/amino_acid_ref_conflicts.csv'
ALLELE_REF_CONFLICTS_FILE = '/tmp/allele_ref_conflicts.csv'


class VariantsMutationsCombinedParser(FileParser):

    def __init__(self, variants_filename: str, mutations_filename: str):
        self.variants_filename = variants_filename
        self.mutations_filename = mutations_filename
        self.delimiter = '\t'

        # find out where our files are (are we in a container or not?) and get absolute paths
        self.mutations_filename_relative, self.mutations_filename_local = (
            self._find_relative_and_local_abs_paths(self.mutations_filename)
        )
        self.variants_filename_relative, self.variants_filename_local = (
            self._find_relative_and_local_abs_paths(self.variants_filename)
        )

        # we also need paths relative to the data directory for use in the db container

        try:
            self._verify_headers()
        except ValueError:
            # Swap arguments and try again
            hold = self.variants_filename_local
            self.variants_filename_local = self.mutations_filename_local
            self.mutations_filename_local = hold
            self._verify_headers()
            # if that worked, we also want these swapped
            hold = self.variants_filename_relative
            self.variants_filename_relative = self.mutations_filename_relative
            self.mutations_filename_relative = hold

        # get orders of headers
        self.variants_header_order = self._get_header_order(
            self.variants_filename_local,
            self.variants_column_mapping
        )
        self.mutations_header_order = self._get_header_order(
            self.mutations_filename_local,
            self.mutations_column_mapping
        )

    async def parse_and_insert(self):
        print(f'{self._get_timestamp()} read mutations')
        await self._read_mutations_input()
        print(f'{self._get_timestamp()} read variants')
        await self._read_variants_input()
        print(f'{self._get_timestamp()} insert alleles')
        await self._insert_alleles()
        print(f'{self._get_timestamp()} allele ref conflicts')
        await self._write_allele_ref_conflicts()
        print(f'{self._get_timestamp()} insert amino acids')
        await self._insert_amino_acids()
        print(f'{self._get_timestamp()} amino acid ref conflicts')
        await self._write_amino_acid_ref_conflicts()
        print(f'{self._get_timestamp()} insert variants')
        await self._insert_variants()
        print(f'{self._get_timestamp()} insert mutations')
        await self._insert_mutations()
        print(f'{self._get_timestamp()} insert intra host translations')
        await self._insert_intra_host_translations()
        print(f'{self._get_timestamp()} insert mutation translations')
        await self._insert_mutation_translations()
        print(f'{self._get_timestamp()} clean up tmp tables')
        await self._clean_up_tmp_tables()
        print(f'Finished at {self._get_timestamp()}')

    async def _read_mutations_input(self):
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    f'''
                    create table tmp_mutations
                    (
                        id          bigserial not null primary key,
                        accession   text      not null,
                        region      text      not null,
                        position_nt int       not null,
                        ref_nt      text,
                        alt_nt      text      not null,
                        gff_feature text,
                        ref_codon   text,
                        alt_codon   text,
                        ref_aa      text,
                        alt_aa      text,
                        position_aa int
                    );
                '''
                )
            )
            await session.execute(
                text(
                    f'''
                        copy tmp_mutations (
                            accession, region, position_nt, ref_nt, alt_nt, gff_feature, 
                            ref_codon, alt_codon, ref_aa, alt_aa, position_aa
                        )
                        from '/muninn/data/{self.mutations_filename_relative}' delimiter E'{self.delimiter}' csv header;
                        '''
                )
            )
            await session.execute(
                text(
                    '''
                    create index idx_tmp_mutations_accession on tmp_mutations (accession);
                    '''
                )
            )
            await session.execute(
                text(
                    '''
                    delete from tmp_mutations
                    where accession not in (
                        select accession
                        from samples
                    );
                    '''
                )
            )
            # todo: info and warning about this
            await session.execute(
                text(
                    '''
                    delete from tmp_mutations where ref_nt is null;
                    '''
                )
            )

            await session.commit()

    async def _read_variants_input(self):
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    '''
                    create table tmp_variants
                    (
                        id          bigserial primary key,
                        region      text  not null,
                        position_nt int   not null,
                        ref_nt      text  not null,
                        alt_nt      text  not null,
                        ref_dp      int   not null,
                        ref_rv      int   not null,
                        ref_qual    float not null,
                        alt_dp      int   not null,
                        alt_rv      int   not null,
                        alt_qual    float not null,
                        alt_freq    float not null,
                        total_dp    int   not null,
                        pval        float not null,
                        pass_qc     bool  not null,
                        gff_feature text,
                        ref_codon   text,
                        ref_aa      text,
                        alt_codon   text,
                        alt_aa      text,
                        position_aa      int,
                        accession   text  not null
                    );
                    '''
                )
            )

            await session.execute(
                text(
                    f'''
                    copy tmp_variants (
                        region, position_nt, ref_nt, alt_nt, ref_dp, ref_rv, ref_qual, 
                        alt_dp, alt_rv, alt_qual, alt_freq, total_dp, pval, pass_qc, gff_feature, 
                        ref_codon, ref_aa, alt_codon, alt_aa, position_aa, accession
                    )
                    from '/muninn/data/{self.variants_filename_relative}' delimiter E'{self.delimiter}' csv header;
                    '''
                )
            )

            await session.execute(
                text(
                    '''
                    create index idx_tmp_variants_accession on tmp_variants (accession);
                    '''
                )
            )

            await session.execute(
                text(
                    '''
                    delete
                    from tmp_variants
                    where accession not in (
                        select accession
                        from samples
                    );
                    '''
                )
            )
            await session.commit()

    @staticmethod
    async def _insert_alleles():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    '''
                    create table tmp_alleles
                    (
                        region      text not null,
                        position_nt int  not null,
                        ref_nt      text not null,
                        alt_nt      text not null
                    );
                    '''
                )
            )

            await session.execute(
                text(
                    '''
                    create unique index uq_tmp_alleles_all on tmp_alleles (region, position_nt, alt_nt, ref_nt);
                    '''
                )
            )

            await session.execute(
                text(
                    '''
                    insert into tmp_alleles (region, position_nt, ref_nt, alt_nt)
                    select region, position_nt, ref_nt, alt_nt
                    from tmp_mutations
                    on conflict (region, position_nt, alt_nt, ref_nt) do nothing;
                    '''
                )
            )

            await session.execute(
                text(
                    '''
                    insert into tmp_alleles (region, position_nt, ref_nt, alt_nt)
                    select region, position_nt, ref_nt, alt_nt
                    from tmp_variants
                    on conflict (region, position_nt, alt_nt, ref_nt) do nothing;
                    '''
                )
            )

            await session.execute(
                text(
                    '''
                    insert into alleles (region, position_nt, ref_nt, alt_nt)
                    select *
                    from tmp_alleles
                    on conflict (region, position_nt, alt_nt) do nothing;
                    '''
                )
            )

            await session.commit()

    @staticmethod
    async def _write_allele_ref_conflicts():
        async with get_async_session() as session:
            res = await session.execute(
                text(
                    '''
                    select combo.region, combo.position_nt, combo.alt_nt, combo.ref_nt, count(*) from
                    (
                        select region, position_nt, alt_nt
                        from (
                            select region, position_nt, alt_nt, count(*)
                            from tmp_alleles
                            group by region, position_nt, alt_nt
                        ) _
                        where _.count > 1
                    ) dups
                    inner join (
                        (
                            select region, position_nt, alt_nt, ref_nt
                            from tmp_mutations tmut
                        )
                        union all
                        (
                            select region, position_nt, alt_nt, ref_nt
                            from tmp_variants tvar
                        )
                    ) combo on dups.region = combo.region and dups.position_nt = combo.position_nt and dups.alt_nt = combo.alt_nt
                    group by combo.region, combo.position_nt, combo.alt_nt, combo.ref_nt ;
                    '''
                )
            )
        conflicts = res.mappings().all()
        with open(ALLELE_REF_CONFLICTS_FILE, 'w+') as f:
            if len(conflicts) > 0:
                impact = sum([c['count'] for c in conflicts])
                print(
                    f'Warning: {len(conflicts)} allele ref conflicts found, '
                    f'impacting {impact} mutation/variant records. See {ALLELE_REF_CONFLICTS_FILE}'
                )
                writer = csv.DictWriter(f, fieldnames=conflicts[0].keys())
                writer.writeheader()
                writer.writerows(conflicts)
            else:
                print('no conflicts found', file=f)

    @staticmethod
    async def _insert_amino_acids():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    '''
                    create table tmp_amino_acids
                    (
                        gff_feature text not null,
                        ref_aa      text not null,
                        alt_aa      text not null,
                        position_aa      int  not null,
                        ref_codon text not null,
                        alt_codon text not null
                    );
                    '''
                )
            )

            await session.execute(
                text(
                    '''
                    create unique index uq_tmp_amino_acids_all on tmp_amino_acids 
                    (gff_feature, position_aa, alt_aa, ref_aa, ref_codon, alt_codon);
                    '''
                )
            )

            await session.execute(
                text(
                    '''
                    insert into tmp_amino_acids (gff_feature, ref_aa, alt_aa, position_aa, ref_codon, alt_codon)
                    select gff_feature, ref_aa, alt_aa, position_aa, ref_codon, alt_codon
                    from tmp_mutations
                    where num_nulls(gff_feature, ref_aa, alt_aa, position_aa) = 0
                    on conflict (gff_feature, ref_aa, alt_aa, position_aa, ref_codon, alt_codon) do nothing;
                    '''
                )
            )

            await session.execute(
                text(
                    '''
                    insert into tmp_amino_acids (gff_feature, ref_aa, alt_aa, position_aa, ref_codon, alt_codon)
                    select gff_feature, ref_aa, alt_aa, position_aa, ref_codon, alt_codon
                    from tmp_variants
                    where num_nulls(gff_feature, ref_aa, alt_aa, position_aa) = 0
                    on conflict (gff_feature, ref_aa, alt_aa, position_aa, ref_codon, alt_codon) do nothing;
                    '''
                )
            )

            await session.execute(
                text(
                    '''
                    insert into amino_acids (position_aa, ref_aa, alt_aa, gff_feature, ref_codon, alt_codon)
                    select position_aa, ref_aa, alt_aa, gff_feature, ref_codon, alt_codon
                    from tmp_amino_acids
                    on conflict (gff_feature, position_aa, alt_aa, alt_codon) do nothing;
                    '''
                )
            )

            await session.commit()

    @staticmethod
    async def _write_amino_acid_ref_conflicts():
        async with get_async_session() as session:
            res = await session.execute(
                text(
                    '''
                    select  combo.gff_feature, combo.position_aa,  combo.alt_aa,  combo.alt_codon, combo.ref_aa, combo.ref_codon, count(*) from
                    (
                        select * from (
                        select gff_feature, position_aa, alt_aa, alt_codon, count(*)
                        from tmp_amino_acids
                        group by gff_feature, position_aa, alt_aa, alt_codon
                    )_ where _.count > 1
                    ) dups
                    inner join (
                        (
                            select gff_feature, position_aa, ref_aa, alt_aa, ref_codon, alt_codon
                            from tmp_mutations tmut
                        )
                        union all
                        (
                            select gff_feature, position_aa, ref_aa, alt_aa, ref_codon, alt_codon
                            from tmp_variants tvar
                        )
                    ) combo on dups.gff_feature = combo.gff_feature and dups.position_aa = combo.position_aa and dups.alt_aa = combo.alt_aa and dups.alt_codon = combo.alt_codon
                    group by combo.gff_feature, combo.position_aa, combo.alt_aa, combo.alt_codon, combo.ref_aa, combo.ref_codon
                    ;
                    '''
                )
            )
        conflicts = res.mappings().all()
        with open(AMINO_ACID_REF_CONFLICTS_FILE, 'w+') as f:
            if len(conflicts) > 0:
                impact = sum([c['count'] for c in conflicts])
                print(
                    f'Warning: {len(conflicts)} amino acid ref conflicts found, '
                    f'impacting {impact} mutation/variant records. See {AMINO_ACID_REF_CONFLICTS_FILE}'
                )
                writer = csv.DictWriter(f, fieldnames=conflicts[0].keys())
                writer.writeheader()
                writer.writerows(conflicts)
            else:
                print('no conflicts found', file=f)

    @staticmethod
    async def _insert_mutations():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    '''
                    insert into mutations (sample_id, allele_id)
                    select distinct s.id, a.id from tmp_mutations tmut
                    left join alleles a on a.region = tmut.region and a.position_nt = tmut.position_nt and a.alt_nt = tmut.alt_nt
                    left join samples s on s.accession = tmut.accession
                    on conflict (sample_id, allele_id) do nothing;
                    '''
                )
            )
            await session.commit()

    @staticmethod
    async def _insert_variants():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    '''
                    insert into intra_host_variants (
                        sample_id, allele_id, ref_dp, alt_dp, alt_freq, ref_rv, alt_rv, ref_qual, alt_qual, total_dp, pval, pass_qc
                    )
                    select distinct on (s.id, a.id)
                        s.id as sample_id,
                        a.id as allele_id,
                        tvar.ref_dp,
                        tvar.alt_dp,
                        tvar.alt_freq,
                        tvar.ref_rv,
                        tvar.alt_rv,
                        tvar.ref_qual,
                        tvar.alt_qual,
                        tvar.total_dp,
                        tvar.pval,
                        tvar.pass_qc
                    from tmp_variants tvar
                    left join alleles a on a.region = tvar.region and a.position_nt = tvar.position_nt and a.alt_nt = tvar.alt_nt
                    left join samples s on s.accession = tvar.accession
                    on conflict (sample_id, allele_id) do update
                        set ref_dp   = excluded.ref_dp,
                            alt_dp   = excluded.alt_dp,
                            alt_freq = excluded.alt_freq,
                            ref_rv   = excluded.ref_rv,
                            alt_rv   = excluded.alt_rv,
                            ref_qual = excluded.ref_qual,
                            alt_qual = excluded.alt_qual,
                            total_dp = excluded.total_dp,
                            pval     = excluded.pval,
                            pass_qc  = excluded.pass_qc;
                    '''
                )
            )
            await session.commit()

    @staticmethod
    async def _insert_mutation_translations():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    '''
                    insert into mutation_translations (mutation_id, amino_acid_id)
                    select distinct  m.id as mutation_id, aa.id as amino_acid_id from tmp_mutations tmut
                    left join amino_acids aa on aa.gff_feature = tmut.gff_feature and aa.position_aa = tmut.position_aa and aa.alt_aa = tmut.alt_aa and aa.alt_codon = tmut.alt_codon
                    left join samples s on s.accession = tmut.accession
                    left join alleles a on a.region = tmut.region and a.position_nt = tmut.position_nt and a.alt_nt = tmut.alt_nt
                    left join mutations m on m.sample_id = s.id and m.allele_id = a.id
                    where num_nulls(tmut.gff_feature, tmut.position_aa, tmut.alt_aa, tmut.ref_aa) = 0
                    on conflict (mutation_id, amino_acid_id) do nothing;
                    '''
                )
            )
            await session.commit()

    @staticmethod
    async def _insert_intra_host_translations():
        async with get_async_write_session() as session:
            await session.execute(
                text(
                    '''
                    insert into intra_host_translations (intra_host_variant_id, amino_acid_id)
                    select distinct ihv.id as intra_host_variant_id, aa.id as amino_acid_id from tmp_variants tvar
                    left join amino_acids aa on aa.gff_feature = tvar.gff_feature and aa.position_aa = tvar.position_aa and aa.alt_aa = tvar.alt_aa and aa.alt_codon = tvar.alt_codon
                    left join samples s on s.accession = tvar.accession
                    left join alleles a on a.region = tvar.region and a.position_nt = tvar.position_nt and a.alt_nt = tvar.alt_nt
                    left join intra_host_variants ihv on ihv.sample_id = s.id and ihv.allele_id = a.id
                    where num_nulls(tvar.gff_feature, tvar.position_aa, tvar.alt_aa, tvar.ref_aa) = 0
                    on conflict (intra_host_variant_id, amino_acid_id) do nothing;
                    '''
                )
            )
            await session.commit()

    @staticmethod
    async def _clean_up_tmp_tables():
        async with get_async_write_session() as session:
            for t in ['tmp_mutations', 'tmp_variants', 'tmp_alleles', 'tmp_amino_acids']:
                await session.execute(
                    text(f'drop table if exists {t};')
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
    def _find_relative_and_local_abs_paths(filename: str) -> (str, str):
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

    def _verify_headers(self):
        with open(self.variants_filename_local, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            required_columns = set(self.variants_column_mapping.values())
            if not set(reader.fieldnames) >= required_columns:
                raise ValueError(f'Missing required fields: {required_columns - set(reader.fieldnames)}')

        with open(self.mutations_filename_local, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            required_columns = set(self.mutations_column_mapping.values())
            if not set(reader.fieldnames) >= required_columns:
                raise ValueError(f'Missing required fields: {required_columns - set(reader.fieldnames)}')

    @staticmethod
    def _get_timestamp():
        return datetime.now().isoformat(timespec='seconds')

    variants_column_mapping = {
        StandardColumnNames.region: 'REGION',
        StandardColumnNames.position_nt: 'POS',
        StandardColumnNames.ref_nt: 'REF',
        StandardColumnNames.alt_nt: 'ALT',
        StandardColumnNames.position_aa: 'POS_AA',
        StandardColumnNames.ref_aa: 'REF_AA',
        StandardColumnNames.alt_aa: 'ALT_AA',
        StandardColumnNames.gff_feature: 'GFF_FEATURE',
        StandardColumnNames.ref_codon: 'REF_CODON',
        StandardColumnNames.alt_codon: 'ALT_CODON',
        StandardColumnNames.accession: 'SRA',
        StandardColumnNames.pval: 'PVAL',
        StandardColumnNames.ref_dp: 'REF_DP',
        StandardColumnNames.ref_rv: 'REF_RV',
        StandardColumnNames.ref_qual: 'REF_QUAL',
        StandardColumnNames.alt_dp: 'ALT_DP',
        StandardColumnNames.alt_rv: 'ALT_RV',
        StandardColumnNames.alt_qual: 'ALT_QUAL',
        StandardColumnNames.pass_qc: 'PASS',
        StandardColumnNames.alt_freq: 'ALT_FREQ',
        StandardColumnNames.total_dp: 'TOTAL_DP',
    }

    mutations_column_mapping = {
        StandardColumnNames.accession: 'sra',
        StandardColumnNames.position_nt: 'pos',
        StandardColumnNames.ref_nt: 'ref',
        StandardColumnNames.alt_nt: 'alt',
        StandardColumnNames.region: 'region',
        StandardColumnNames.gff_feature: 'GFF_FEATURE',
        StandardColumnNames.ref_codon: 'ref_codon',
        StandardColumnNames.alt_codon: 'alt_codon',
        StandardColumnNames.ref_aa: 'ref_aa',
        StandardColumnNames.alt_aa: 'alt_aa',
        StandardColumnNames.position_aa: 'pos_aa',
    }
