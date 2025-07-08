import csv
from csv import DictReader
from enum import Enum
from typing import Set

from DB.inserts.annotations import find_or_insert_annotation
from DB.inserts.annotations_papers import find_or_insert_annotation_paper
from DB.inserts.effects import find_or_insert_effect
from DB.inserts.file_parsers.file_parser import FileParser

from DB.inserts.papers import find_or_insert_paper
from DB.models import AminoAcid, Effect, Paper, Annotation, AnnotationPaper
from DB.queries.amino_acid_substitutions import find_aa_sub
from utils.constants import DefaultGffFeaturesByRegion
from utils.csv_helpers import parse_change_string, get_value
from utils.errors import NotFoundError

# this is intended to parse the output of the following query:
# select mm.mutation_name,
#        me.effect_name,
#        p.title,
#        p.authors,
#        p.year
# from markers_effects me
# inner join papers p on p.id = me.paper_id
# inner join markers mk on mk.id = me.marker_id
# inner join markers_mutations mm on mm.marker_id = mk.id
# where me.subtype = 'H5N1';

UNMAPPED = 'unmapped'

class FlumutParser(FileParser):

    def __init__(self, filename: str, delimiter: str):
        self.filename = filename
        self.delimiter = delimiter

    async def parse_and_insert(self):
        debug_info = {
            'count_skipped_malformed': 0,
            'count_skipped_no_gff_mapping': 0,
            'count_skipped_amino_acid_not_found': 0,
            'proteins_not_mapped': set(),
        }
        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            FlumutParser._verify_header(reader)
            for row in reader:
                try:
                    protein, ref, position, alt = get_value(
                        row,
                        ColNameMapping.aa_change.value,
                        transform=parse_change_string
                    )
                    paper_title = get_value(row, ColNameMapping.paper_title.value)
                    paper_authors = get_value(row, ColNameMapping.paper_authors.value)
                    paper_year = get_value(row, ColNameMapping.paper_year.value, transform=int)
                    effect_name = get_value(row, ColNameMapping.effect_detail.value)
                except ValueError:
                    debug_info['count_skipped_malformed'] += 1
                    continue

                gff_mapping = {
                    'HA1-5': 'XAJ25415.1',
                    'HA2-5': 'XAJ25415.1',
                    'M1': 'XAJ25416.1',
                    'M2': 'XAJ25417.1',
                    'NA-1': 'XAJ25418.1',  # todo: ?
                    'NP': 'XAJ25419.1',
                    'NS-1': 'XAJ25420.1',
                    'NS-2': UNMAPPED,
                    'PA': ' XAJ25422.1',
                    'PB1': 'XAJ25424.1',
                    'PB1-F2': UNMAPPED,  # todo: ?
                    'PB2': 'XAJ25426.1',
                }

                try:
                    gff_feature = gff_mapping[protein]
                except KeyError:
                    debug_info['count_skipped_no_gff_mapping'] += 1
                    debug_info['proteins_not_mapped'].add(protein)
                    continue
                if gff_feature == UNMAPPED:
                    debug_info['count_skipped_no_gff_mapping'] += 1
                    debug_info['proteins_not_mapped'].add(protein)
                    continue

                # amino acid
                # position adjustment
                if gff_feature == DefaultGffFeaturesByRegion.HA:
                    position += 16
                try:
                    aa_id = await find_aa_sub(
                        AminoAcid(
                            gff_feature=gff_feature,
                            position_aa=position,
                            alt_aa=alt,
                            ref_aa=ref
                        )
                    )
                except NotFoundError:
                    debug_info['count_skipped_amino_acid_not_found'] += 1
                    continue

                # effect
                effect_id = await find_or_insert_effect(
                    Effect(
                        detail=effect_name
                    )
                )

                # paper
                paper_id = await find_or_insert_paper(
                    Paper(
                        title=paper_title,
                        authors=paper_authors,
                        publication_year=paper_year
                    )
                )

                # annotation
                annotation_id = await find_or_insert_annotation(
                    Annotation(
                        amino_acid_id=aa_id,
                        effect_id=effect_id
                    )
                )

                # annotation-paper
                await find_or_insert_annotation_paper(
                    AnnotationPaper(
                        annotation_id=annotation_id,
                        paper_id=paper_id
                    )
                )
        print(debug_info)

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return {cn.value for cn in {
            ColNameMapping.aa_change,
            ColNameMapping.effect_detail,
            ColNameMapping.paper_title,
            ColNameMapping.paper_authors,
            ColNameMapping.paper_year,
        }}

    @staticmethod
    def _verify_header(reader: DictReader):
        required_columns = FlumutParser.get_required_column_set()
        diff = required_columns - set(reader.fieldnames)
        if not len(diff) == 0:
            raise ValueError(f'The following required columns were not found: {diff}')


class ColNameMapping(Enum):
    aa_change = 'mutation_name'
    effect_detail = 'effect_name'
    paper_title = 'title'
    paper_authors = 'authors'
    paper_year = 'year'


class FlumutTsvParser(FlumutParser):
    def __init__(self, filename: str):
        super().__init__(filename, '\t')
