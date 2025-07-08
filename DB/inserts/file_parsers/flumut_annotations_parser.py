import csv
from typing import Set

from DB.inserts.annotations import find_or_insert_annotation
from DB.inserts.annotations_papers import find_or_insert_annotation_paper
from DB.inserts.effects import find_or_insert_effect
from DB.inserts.file_parsers.file_parser import FileParser
import polars as pl

from DB.inserts.papers import find_or_insert_paper
from DB.models import AminoAcid, Effect, Paper, Annotation, AnnotationPaper
from DB.queries.amino_acid_substitutions import find_aa_sub
from utils.constants import DefaultGffFeaturesByRegion
from utils.csv_helpers import parse_change_string, get_value
from utils.errors import NotFoundError


class FlumutParser(FileParser):

    def __init__(self, filename: str, delimiter: str):
        self.filename = filename
        self.delimiter = delimiter

    async def parse_and_insert(self):
        debug_info = {
            'count_skipped_malformed': 0,
            'count_skipped_no_gff_mapping': 0,
            'count_skipped_amino_acid_not_found': 0,
        }
        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            # todo: check header
            for row in reader:
                try:
                    protein, ref, position, alt = parse_change_string(row['mutation_name'])
                    paper_title = get_value(row, 'title')
                    paper_authors = get_value(row, 'authors')
                    paper_year = get_value(row, 'year', transform=int)
                    effect_name = get_value(row, 'effect_name')
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
                    'NS-2': 'unmapped',
                    'PA': ' XAJ25422.1',
                    'PB1': 'XAJ25424.1',
                    'PB1-F': 'XAJ25424.1',  # todo: ?
                    'PB2': 'XAJ25426.1',
                }

                try:
                    gff_feature = gff_mapping[protein]
                except KeyError:
                    debug_info['count_skipped_no_gff_mapping'] += 1
                    continue
                if gff_feature == 'unmapped':
                    debug_info['count_skipped_no_gff_mapping'] += 1
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
                paper_id = await find_or_insert_paper(Paper(
                    title=paper_title,
                    authors=paper_authors,
                    publication_year=paper_year
                ))

                # annotation
                annotation_id = await find_or_insert_annotation(Annotation(
                    amino_acid_id=aa_id,
                    effect_id=effect_id
                ))

                # annotation-paper
                annot_paper_preexisted = await find_or_insert_annotation_paper(AnnotationPaper(
                    annotation_id=annotation_id,
                    paper_id=paper_id
                ))
        print(debug_info)


    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return required_columns


required_columns = {
    'mutation_name',
    'effect_name',
    'title',
    'authors',
    'year'
}

class FlumutTsvParser(FlumutParser):
    def __init__(self, filename: str):
        super().__init__(filename, '\t')