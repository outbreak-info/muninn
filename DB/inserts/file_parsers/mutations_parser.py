import csv
from csv import DictReader
from enum import Enum

from DB.inserts.alleles import find_or_insert_allele
from DB.inserts.amino_acid_substitutions import find_or_insert_aa_sub
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.mutations import find_or_insert_mutation
from DB.inserts.samples import find_sample_id_by_accession
from DB.inserts.translations import insert_translation
from DB.models import Allele, AminoAcidSubstitution, Translation, Mutation
from utils.csv_helpers import get_value
from utils.errors import NotFoundError


class MutationsTsvParser(FileParser):

    def __init__(self, filename: str):
        self.filename = filename

    async def parse_and_insert(self):
        debug_info = {
            'skipped_malformed': 0,
            'skipped_sample_not_found': 0
        }

        # format = (region, pos, alt) -> id
        cache_alleles = dict()
        # format = (gff_feature, pos, alt) -> id
        cache_aa_subs = dict()
        # format = accession -> id
        cache_samples = dict()
        cache_accessions_not_found = set()
        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            MutationsTsvParser._verify_header(reader)

            for row in reader:
                try:
                    accession = get_value(row, ColNameMapping.accession.value)
                    if accession in cache_accessions_not_found:
                        debug_info['skipped_sample_not_found'] += 1
                        continue

                    region = get_value(row, ColNameMapping.region.value)
                    ref_nt = get_value(row, ColNameMapping.ref_nt.value)
                    ref_nt = get_value(row, ColNameMapping.ref_nt.value)
                    alt_nt = get_value(row, ColNameMapping.alt_nt.value)
                    position_nt = get_value(row, ColNameMapping.position_nt.value, transform=int)

                    allele = Allele(
                        region=region,
                        position_nt=position_nt,
                        ref_nt=ref_nt,
                        alt_nt=alt_nt
                    )

                    try:
                        allele_id = cache_alleles[(region, position_nt, alt_nt)]
                    except KeyError:
                        allele_id = await find_or_insert_allele(allele)
                        cache_alleles[(region, position_nt, alt_nt)] = allele_id

                    try:
                        sample_id = cache_samples[accession]
                    except KeyError:
                        try:
                            sample_id = await find_sample_id_by_accession(accession)
                            cache_samples[accession] = sample_id
                        except NotFoundError:
                            debug_info['skipped_sample_not_found'] += 1
                            cache_accessions_not_found.add(accession)
                            continue

                    # get aa values, if they're all present
                    try:
                        gff_feature = get_value(row, ColNameMapping.gff_feature.value)
                        position_aa = get_value(row, ColNameMapping.position_aa.value, transform=int)
                        ref_aa = get_value(row, ColNameMapping.ref_aa.value)
                        alt_aa = get_value(row, ColNameMapping.alt_aa.value)
                        ref_codon = get_value(row, ColNameMapping.ref_codon.value)
                        alt_codon = get_value(row, ColNameMapping.alt_codon.value)

                        try:
                            aa_sub_id = cache_aa_subs[(gff_feature, position_aa, alt_aa)]
                        except KeyError:
                            aa_sub = AminoAcidSubstitution(
                                gff_feature=gff_feature,
                                position_aa=position_aa,
                                ref_aa=ref_aa,
                                alt_aa=alt_aa,
                                ref_codon=ref_codon,
                                alt_codon=alt_codon,
                            )
                            aa_sub_id = await find_or_insert_aa_sub(aa_sub)
                            cache_aa_subs[(gff_feature, position_aa, alt_aa)] = aa_sub_id

                        await insert_translation(
                            Translation(
                                allele_id=allele_id,
                                amino_acid_substitution_id=aa_sub_id
                            )
                        )
                    except ValueError:
                        pass

                    mutation = Mutation(
                        sample_id=sample_id,
                        allele_id=allele_id
                    )
                    await find_or_insert_mutation(mutation)

                except ValueError:
                    debug_info['skipped_malformed'] += 1

    @classmethod
    def _verify_header(cls, reader: DictReader):
        required_columns = {cn.value for cn in {
            ColNameMapping.accession,
            ColNameMapping.region,
            ColNameMapping.position_nt,
            ColNameMapping.ref_nt,
            ColNameMapping.alt_nt,
            ColNameMapping.gff_feature,
            ColNameMapping.ref_codon,
            ColNameMapping.alt_codon,
            ColNameMapping.ref_aa,
            ColNameMapping.alt_aa,
            ColNameMapping.position_aa,
        }}
        diff = required_columns - set(reader.fieldnames)
        if not len(diff) == 0:
            raise ValueError(f'Not all required columns are present, missing: {diff}')


class ColNameMapping(Enum):
    accession = 'sra'
    position_nt = 'position'
    ref_nt = 'ref'
    alt_nt = 'alt'
    region = 'region'
    gff_feature = 'GFF_FEATURE'
    ref_codon = 'ref_codon'
    alt_codon = 'alt_codon'
    ref_aa = 'ref_aa'
    alt_aa = 'alt_aa'
    position_aa = 'pos_aa'
