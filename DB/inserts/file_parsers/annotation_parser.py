from csv import DictReader
from enum import Enum
from typing import Set

from DB.inserts.amino_acid_substitutions import find_aa_sub, insert_aa_sub
from DB.inserts.effect_insert import find_or_insert_effect
from DB.inserts.annotation_insert import find_or_insert_annotation
from DB.inserts.paper_insert import find_or_insert_paper
from DB.inserts.annotation_paper_insert import insert_annotation_paper
from DB.inserts.file_parsers.file_parser import FileParser
from DB.models import AminoAcidSubstitution, Paper, Annotation_Paper, Annotation, Effect
from utils.csv_helpers import get_value
from utils.errors import NotFoundError

NEW_AAS_LIMIT = 0

class AnnotationsFileParser(FileParser):

    def __init__(self, filename: str, delimiter: str):
        self.filename = filename
        self.delimiter = delimiter

    async def parse_and_insert(self):
        debug_info = {
            'skipped_aas_data_missing': 0,
            'skipped_aas_not_found': 0,
            'skipped_effect_info_missing': 0,
            'skipped_paper_info_missing': 0,
            'annotation_paper_pair_exists': 0
        }
        # format = (title,year) -> id
        cache_paper_ids = dict()
        # format = detail -> id
        cache_effect_ids = dict()
        # format = (aas_id, effect_id) -> id
        cache_annotation_ids = dict()
        # format = (gff_feature, position_aa, ref_aa, alt_aa) -> id
        cache_amino_sub_ids = dict()
        # format = (gff_feature, position_aa, ref_aa, alt_aa) -> id
        cache_new_amino_sub_ids = dict()
        # format = (gff_feature, position_aa, ref_aa, alt_aa) -> id
        cache_amino_sub_ids = dict()
        cache_amino_subs_not_found = set()
        with open(self.filename, 'r') as f:
            reader = DictReader(f, delimiter=self.delimiter)
            self._verify_header(reader)

            for row in reader:

                try:
                    position_aa = get_value(row, ColNameMapping.position_aa.value, transform=int)
                    ref_aa = get_value(row, ColNameMapping.ref_aa.value)
                    alt_aa = get_value(row, ColNameMapping.alt_aa.value)
                    gff_feature = get_value(row, ColNameMapping.gff_feature.value)
                except ValueError:
                    debug_info['skipped_aas_info_missing'] += 1
                    continue
                
                stripped_gff_feature = gff_feature[4:]

                if (gff_feature, position_aa, ref_aa, alt_aa) in cache_amino_subs_not_found and (stripped_gff_feature, position_aa, ref_aa, alt_aa) in cache_amino_subs_not_found:
                    debug_info['skipped_aas_not_found'] += 1
                    continue
                try:
                    aas_id = cache_amino_sub_ids[(gff_feature, position_aa, ref_aa, alt_aa)]
                except KeyError:
                    try:
                        aas_id = await find_aa_sub(
                            AminoAcidSubstitution(
                                gff_feature=gff_feature,
                                position_aa=position_aa,
                                alt_aa=alt_aa,
                                ref_aa=ref_aa
                            )
                        )
                        cache_amino_sub_ids[(gff_feature, position_aa, ref_aa, alt_aa)] = aas_id
                    except NotFoundError:
                        try:
                            aas_id = await find_aa_sub(
                                AminoAcidSubstitution(
                                    gff_feature=stripped_gff_feature,
                                    position_aa=position_aa,
                                    alt_aa=alt_aa,
                                    ref_aa=ref_aa
                                )
                            )
                            cache_amino_sub_ids[(stripped_gff_feature, position_aa, ref_aa, alt_aa)] = aas_id
                        except NotFoundError:
                                if len(cache_new_amino_sub_ids) >= NEW_AAS_LIMIT:
                                    debug_info['skipped_aas_not_found'] += 1
                                    cache_amino_subs_not_found.add((gff_feature, position_aa, ref_aa, alt_aa))
                                    cache_amino_subs_not_found.add((stripped_gff_feature, position_aa, ref_aa, alt_aa))
                                    continue
                                aas_id = await insert_aa_sub(
                                    AminoAcidSubstitution(
                                    gff_feature=gff_feature,
                                    position_aa=position_aa,
                                    alt_aa=alt_aa,
                                    ref_aa=ref_aa,
                                    ref_codon='AAA',
                                    alt_codon='AAA'
                                    )
                                )
                                cache_new_amino_sub_ids[(gff_feature,position_aa,ref_aa,alt_aa)] = aas_id

                try:
                    detail = get_value(row, ColNameMapping.detail.value)
                except ValueError:
                    debug_info['skipped_effect_info_missing'] += 1
                    continue
                
                try:
                    effect_id = cache_effect_ids[detail]
                except KeyError:
                    effect_id = await find_or_insert_effect(
                        Effect(
                            detail=detail
                        )
                    )
                    cache_effect_ids[detail] = effect_id

                try:
                    annotation_id = cache_annotation_ids[(aas_id,effect_id)]
                except KeyError:
                    annotation_id = await find_or_insert_annotation(
                        Annotation(
                            amino_acid_substitution_id=aas_id,
                            effect_id=effect_id 
                        )
                    )
                    cache_annotation_ids[(aas_id,effect_id)] = annotation_id

                try:
                    author = get_value(row, ColNameMapping.author.value)
                    publication_year = get_value(row,ColNameMapping.publication_year.value, transform=int)
                except ValueError:
                    debug_info['skipped_paper_info_missing'] += 1
                    continue
                
                try:
                    paper_id = cache_annotation_ids[(author,publication_year)]
                except KeyError:
                    paper_id = await find_or_insert_paper(
                        Paper(
                            author=author,
                            publication_year=publication_year
                        )
                    )
                    cache_paper_ids[(author,publication_year)] = paper_id
                existing: Annotation_Paper = await insert_annotation_paper(
                    Annotation_Paper(
                        annotation_id=annotation_id,
                        paper_id=paper_id
                    )
                )

                if existing is not None:
                    debug_info['annotation_paper_pair_exists'] += 1
        print(debug_info)

    @classmethod
    def _verify_header(cls, reader: DictReader):
        required_cols = cls.get_required_column_set()
        diff = required_cols - set(reader.fieldnames)
        if not len(diff) == 0:
            raise ValueError(f'Not all required columns are present, missing: {diff}')

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        cols = {cn.value for cn in {
            ColNameMapping.gff_feature,
            ColNameMapping.position_aa,
            ColNameMapping.alt_aa,
            ColNameMapping.ref_aa,
            ColNameMapping.publication_year,
            ColNameMapping.author,
            ColNameMapping.detail
            }}
        return cols


class ColNameMapping(Enum):
    ref_aa = 'ref_aa'
    alt_aa = 'alt_aa'
    position_aa = 'position_aa'
    gff_feature = 'gff_feature'
    publication_year = 'publication_year'
    author = 'author'
    detail = 'detail'




class AnnotationParser(AnnotationsFileParser):
    def __init__(self, filename: str):
        super().__init__(filename, ',')

    async def parse_and_insert(self):
        await super().parse_and_insert()
