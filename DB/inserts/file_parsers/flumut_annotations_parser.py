import csv
from csv import DictReader
from enum import Enum
from typing import Set, List
import polars as pl

from DB.inserts.annotations import insert_annotation
from DB.inserts.annotations_papers import find_or_insert_annotation_paper
from DB.inserts.effects import find_or_insert_effect
from DB.inserts.file_parsers.file_parser import FileParser

from DB.inserts.papers import find_or_insert_paper
from DB.models import AminoAcid, Effect, Paper, Annotation, AnnotationPaper
from DB.inserts.amino_acids import find_amino_acid
from utils.constants import DefaultGffFeaturesByRegion
from utils.csv_helpers import parse_change_string, get_value
from utils.errors import NotFoundError, DuplicateAnnotationError

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


class FlumutParser(FileParser):

    def __init__(self, filename: str, delimiter: str):
        self.filename = filename
        self.delimiter = delimiter
        self._verify_header()

    async def parse_and_insert(self):
        annotation_id_by_marker_id = dict()

        debug_info = {
            'count_skipped_misc': 0,
            'count_skipped_duplicate_annotation': 0,
            'count_skipped_aa_not_found': 0
        }

        annotations_input = pl.scan_csv(
            self.filename,
            separator=self.delimiter,
            with_column_names= lambda names : [ColNameMapping(n).name for n in names]
        ).collect()

        # Each entry in this df will correspond to one row in the annotations table
        uq_effects_markers = (
            annotations_input
            .select(
                pl.col(ColNameMapping.marker_id.name),
                pl.col(ColNameMapping.effect_detail.name)
            )
            .unique()
        )

        for marker_id, effect_detail in uq_effects_markers.rows():
            # Get all the related rows from the main df
            one_annotation = annotations_input.filter(
                pl.col(ColNameMapping.effect_detail.name) == effect_detail,
                pl.col(ColNameMapping.marker_id.name) == marker_id
            )

            # get the amino acid data
            amino_acid_ids: List[int] = []
            try:
                for aa_change in one_annotation.select(pl.col(ColNameMapping.aa_change.name)).rows():
                    protein, ref, position, alt = parse_change_string(aa_change[0])
                    gff_feature = gff_mapping[protein]

                    # adjust position for HA
                    if gff_feature == DefaultGffFeaturesByRegion.HA:
                        position += 16

                    amino_acid_ids.append(
                        await find_amino_acid(
                            AminoAcid(
                                gff_feature=gff_feature,
                                position_aa=position,
                                ref_aa=ref,
                                alt_aa=alt
                            )
                        )
                    )
            except (KeyError, ValueError):
                debug_info['count_skipped_misc'] += 1
                continue
            except NotFoundError:
                debug_info['count_skipped_aa_not_found'] += 1
                continue

            # effect
            effect_id = await find_or_insert_effect(
                Effect(
                    detail=effect_detail
                )
            )

            # papers
            paper_ids: List[int] = []
            for title, authors, year in one_annotation.select(
                    pl.col(ColNameMapping.paper_title.name),
                    pl.col(ColNameMapping.paper_authors.name),
                    pl.col(ColNameMapping.paper_year.name),
            ).rows():
                paper_ids.append(
                    await find_or_insert_paper(
                        Paper(
                            title=title,
                            authors=authors,
                            publication_year=year
                        )
                    )
                )

            # Insert annotation
            # this also handles the amino acids relationships
            try:
                annotation_id = await insert_annotation(
                    Annotation(
                        effect_id=effect_id
                    ),
                    set(amino_acid_ids)
                )
            except DuplicateAnnotationError:
                debug_info['count_skipped_duplicate_annotation'] += 1
                continue

            # associate with papers
            for pid in paper_ids:
                await find_or_insert_annotation_paper(
                    AnnotationPaper(
                        annotation_id=annotation_id,
                        paper_id=pid
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
            ColNameMapping.marker_id
        }}

    def _verify_header(self):
        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
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
    marker_id = 'marker_id'


class FlumutTsvParser(FlumutParser):
    def __init__(self, filename: str):
        super().__init__(filename, '\t')
