from csv import DictReader
from enum import Enum

from DB.inserts.amino_acid_substitutions import find_aa_sub
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.phenotype_measurement_results import insert_pheno_measurement_result
from DB.inserts.phenotype_metrics import find_or_insert_metric
from DB.models import AminoAcidSubstitution, PhenotypeMetric, PhenotypeMeasurementResult
from utils.constants import PhenotypeMetricAssayTypes, DefaultGffFeaturesByRegion
from utils.csv_helpers import get_value
from utils.errors import NotFoundError


class DmsFileParser(FileParser):

    def __init__(self, filename: str, delimiter: str, gff_feature: str):
        self.filename = filename
        self.delimiter = delimiter
        self.gff_feature = gff_feature

        # this will allow us to pass a set of data cols as a parameter later if needed
        # todo: these names and values need to be canonized and controlled.
        #  The names here will be used as metric names in the DB, and the values are the expected col names in input.
        #  So messing up the variable names would mess up a lot.
        #  Any scripts doing these insertions need to be sharing a single name mapping to avoid a mess.
        self.data_cols = {
            ColNameMapping.ferret_sera_escape,
            ColNameMapping.mouse_sera_escape,
            ColNameMapping.species_sera_escape,
            ColNameMapping.entry_in_293t_cells,
            ColNameMapping.stability,
            ColNameMapping.sa26_usage_increase,
            ColNameMapping.mature_h5_site
        }

    async def parse_and_insert(self):
        debug_info = {
            'skipped_aas_data_missing': 0,
            'skipped_aas_not_found': 0,
            'value_parsing_errors': 0,
            'count_existing_updated': 0 # only counts if the value changed
        }
        # format = metric_name -> id
        cache_metric_ids = dict()
        # format = (position_aa, ref_aa, alt_aa) -> id
        cache_amino_sub_ids = dict()
        # format = (position_aa, ref_aa, alt_aa)
        cache_amino_subs_not_found = set()
        with open(self.filename, 'r') as f:
            reader = DictReader(f, delimiter=self.delimiter)
            self._verify_header(reader)

            for row in reader:

                try:
                    position_aa = get_value(row, ColNameMapping.position_aa.value, transform=int)
                    ref_aa = get_value(row, ColNameMapping.ref_aa.value)
                    alt_aa = get_value(row, ColNameMapping.alt_aa.value)
                except ValueError:
                    debug_info['skipped_aas_info_missing'] += 1
                    continue

                if (position_aa, ref_aa, alt_aa) in cache_amino_subs_not_found:
                    debug_info['skipped_aas_not_found'] += 1
                    continue
                try:
                    aas_id = cache_amino_sub_ids[(position_aa, ref_aa, alt_aa)]
                except KeyError:
                    try:
                        aas_id = await find_aa_sub(
                            AminoAcidSubstitution(
                                gff_feature=self.gff_feature,
                                position_aa=position_aa,
                                alt_aa=alt_aa,
                                ref_aa=ref_aa
                            )
                        )
                        cache_amino_sub_ids[(position_aa, ref_aa, alt_aa)] = aas_id
                    except NotFoundError:
                        # if the aas doesn't already exist, skip the record.
                        # we don't want to create orphaned aas entries just for the dms data
                        debug_info['skipped_aas_not_found'] += 1
                        cache_amino_subs_not_found.add((position_aa, ref_aa, alt_aa))
                        continue

                for data_col in self.data_cols:
                    try:
                        v = get_value(row, data_col.value, transform=float)
                    except ValueError:
                        debug_info['value_parsing_errors'] += 1
                        continue

                    try:
                        metric_id = cache_metric_ids[data_col.name]
                    except KeyError:
                        metric_id = await find_or_insert_metric(
                            PhenotypeMetric(
                                name = data_col.name,
                                assay_type=PhenotypeMetricAssayTypes.DMS
                            )
                        )
                        cache_metric_ids[data_col.name] = metric_id


                    updated = await insert_pheno_measurement_result(
                        PhenotypeMeasurementResult(
                            amino_acid_substitution_id=aas_id,
                            phenotype_metric_id=metric_id,
                            value=v
                        ),
                        upsert=True
                    )
                    if updated:
                        debug_info['count_existing_updated'] += 1

        debug_info['count_aas_not_found'] = len(cache_amino_subs_not_found)
        print(debug_info)




    def _verify_header(self, reader: DictReader):
        required_cols = {cn.value for cn in {
            ColNameMapping.position_aa,
            ColNameMapping.alt_aa,
            ColNameMapping.ref_aa,
        }}
        for cn in self.data_cols:
            required_cols.add(cn.value)

        diff = required_cols - set(reader.fieldnames)
        if not len(diff) == 0:
            raise ValueError(f'Not all required columns are present, missing: {diff}')


class ColNameMapping(Enum):
    ref_aa = 'ref'
    alt_aa = 'mutant'
    position_aa = 'pos'
    species_sera_escape = 'species sera escape'
    entry_in_293t_cells = 'entry in 293T cells'
    stability = 'stability'
    sa26_usage_increase = 'SA26 usage increase'
    mature_h5_site = 'mature_H5_site'
    ferret_sera_escape = 'ferret sera escape'
    mouse_sera_escape = 'mouse sera escape'



class HaRegionDmsTsvParser(DmsFileParser):
    def __init__(self, filename: str):
        super().__init__(filename, '\t', DefaultGffFeaturesByRegion.HA)

    async def parse_and_insert(self):
        await super().parse_and_insert()