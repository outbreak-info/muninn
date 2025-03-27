import csv
from csv import DictReader
from enum import Enum

from DB.errors import NotFoundError
from DB.inserts.alleles import find_or_insert_allele
from DB.inserts.amino_acid_substitutions import find_or_insert_aa_sub
from DB.inserts.file_formats.file_format import FileFormat
from DB.inserts.samples import find_sample_id_by_accession
from DB.inserts.variants import find_or_insert_variant
from DB.models import Allele, AminoAcidSubstitution, IntraHostVariant
from utils.csv_helpers import get_value, bool_from_str, int_from_decimal_str


class CombinedTsvV1(FileFormat):

    @classmethod
    async def insert_from_file(cls, filename: str) -> None:
        # create a csv reader spec'd to the header we're expecting

        with open(filename, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            CombinedTsvV1._verify_header(reader)

            for row in reader:
                try:
                    # allele data
                    allele_id = await find_or_insert_allele(
                        Allele(
                            region=row[cls.ColNameMapping.region.value],
                            position_nt=int(row[cls.ColNameMapping.position_nt.value]),
                            ref_nt=row[cls.ColNameMapping.ref_nt.value],
                            alt_nt=row[cls.ColNameMapping.alt_nt.value]
                        )
                    )

                    # amino acid info
                    # should either all be present or all be absent
                    # we use gff_feature as our canary. If it's present and other values are missing, the db will complain
                    gff_feature = get_value(row, cls.ColNameMapping.gff_feature.value, allow_none=True)
                    if gff_feature is not None:
                        await find_or_insert_aa_sub(
                            AminoAcidSubstitution(
                                position_aa=get_value(
                                    row,
                                    cls.ColNameMapping.position_aa.value,
                                    transform=int_from_decimal_str
                                ),
                                ref_aa=(row[cls.ColNameMapping.ref_aa.value]),
                                alt_aa=(row[cls.ColNameMapping.alt_aa.value]),
                                ref_codon=(row[cls.ColNameMapping.ref_codon.value]),
                                alt_codon=(row[cls.ColNameMapping.alt_codon.value]),
                                allele_id=allele_id,
                                gff_feature=gff_feature
                            )
                        )

                    sample_accession = row[cls.ColNameMapping.accession.value]
                    try:
                        sample_id = await find_sample_id_by_accession(sample_accession)
                    except NotFoundError:
                        print(f'sample not found for accession: {sample_accession}')
                        continue
                    # variant data
                    variant = IntraHostVariant(
                        sample_id=sample_id,
                        allele_id=allele_id,
                        pval=get_value(row, cls.ColNameMapping.pval.value, transform=float),
                        ref_dp=get_value(row, cls.ColNameMapping.ref_dp.value, transform=int),
                        alt_dp=get_value(row, cls.ColNameMapping.alt_dp.value, transform=int),
                        ref_rv=get_value(row, cls.ColNameMapping.ref_rv.value, transform=int),
                        alt_rv=get_value(row, cls.ColNameMapping.alt_rv.value, transform=int),
                        ref_qual=get_value(row, cls.ColNameMapping.ref_qual.value, transform=int),
                        alt_qual=get_value(row, cls.ColNameMapping.alt_qual.value, transform=int),
                        pass_qc=get_value(row, cls.ColNameMapping.pass_qc.value, transform=bool_from_str),
                        alt_freq=get_value(row, cls.ColNameMapping.alt_freq.value, transform=float),
                        total_dp=get_value(row, cls.ColNameMapping.total_dp.value, transform=int_from_decimal_str),
                    )

                    _, preexisting = await find_or_insert_variant(variant)

                    if preexisting:
                        # todo: proper logging
                        print(
                            f'Warning, tried to insert two variants for the same sample-allele pair, '
                            f'sample: {sample_id}, allele: {allele_id}'
                        )

                    # todo: deal with dms values

                except KeyError as e:
                    # todo: logging
                    print(f'Malformed row in variants: {row}, {str(e)}')

    class ColNameMapping(Enum):
        region = 'REGION'
        position_nt = 'POS'
        ref_nt = 'REF'
        alt_nt = 'ALT'

        position_aa = 'POS_AA'
        ref_aa = 'REF_AA'
        alt_aa = 'ALT_AA'
        gff_feature = 'GFF_FEATURE'
        ref_codon = 'REF_CODON'
        alt_codon = 'ALT_CODON'

        accession = 'sra'

        pval = 'PVAL'
        ref_dp = 'REF_DP'
        ref_rv = 'REF_RV'
        ref_qual = 'REF_QUAL'
        alt_dp = 'ALT_DP'
        alt_rv = 'ALT_RV'
        alt_qual = 'ALT_QUAL'
        pass_qc = 'PASS'
        alt_freq = 'ALT_FREQ'
        total_dp = 'TOTAL_DP'

        # todo: in theory the way we plan to store dms data should make this silly,
        #  but it needs to be like this for the moment.
        #  Also: there's a difference between letting the schema be flexible about what data we're storing
        #  and needing the file parsing system to be equally flexible about the data contained in the files it's parsing
        species_sera_escape = 'species sera escape'
        entry_in_293t_cells = 'entry in 293T cells'
        stability = 'stability'
        sa26_usage_increase = 'SA26 usage increase'
        sequential_site = 'sequential_site'
        ref_h1_site = 'reference_H1_site'
        mature_h5_site = 'mature_H5_site'
        ha1_ha2_h5_site = 'HA1_HA2_H5_site'
        # region='region' todo
        nt_changes_to_codon = 'nt changes to codon'

    @classmethod
    def _verify_header(cls, reader: DictReader) -> None:
        expected_header = [
            cls.ColNameMapping.region.value,
            cls.ColNameMapping.position_nt.value,
            cls.ColNameMapping.ref_nt.value,
            cls.ColNameMapping.alt_nt.value,
            cls.ColNameMapping.ref_dp.value,
            cls.ColNameMapping.ref_rv.value,
            cls.ColNameMapping.ref_qual.value,
            cls.ColNameMapping.alt_dp.value,
            cls.ColNameMapping.alt_rv.value,
            cls.ColNameMapping.alt_qual.value,
            cls.ColNameMapping.alt_freq.value,
            cls.ColNameMapping.total_dp.value,
            cls.ColNameMapping.pval.value,
            cls.ColNameMapping.pass_qc.value,
            cls.ColNameMapping.gff_feature.value,
            cls.ColNameMapping.ref_codon.value,
            cls.ColNameMapping.ref_aa.value,
            cls.ColNameMapping.alt_codon.value,
            cls.ColNameMapping.alt_aa.value,
            cls.ColNameMapping.position_aa.value,
            cls.ColNameMapping.accession.value,
            cls.ColNameMapping.species_sera_escape.value,
            cls.ColNameMapping.entry_in_293t_cells.value,
            cls.ColNameMapping.stability.value,
            cls.ColNameMapping.sa26_usage_increase.value,
            cls.ColNameMapping.sequential_site.value,
            cls.ColNameMapping.ref_h1_site.value,
            cls.ColNameMapping.mature_h5_site.value,
            cls.ColNameMapping.ha1_ha2_h5_site.value,
            'region',  # todo
            cls.ColNameMapping.nt_changes_to_codon.value
        ]
        if reader.fieldnames != expected_header:
            raise ValueError('did not find expected header')
