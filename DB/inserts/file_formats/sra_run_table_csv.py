import csv
from csv import DictReader
from enum import Enum
from typing import Dict

from dateutil import parser

from DB.inserts.geo_locations import find_or_insert_geo_location
from DB.inserts.file_formats.file_format import FileFormat
from DB.inserts.samples import find_or_insert_sample
from DB.models import GeoLocation, Sample
from utils.csv_helpers import get_value, bool_from_str
from utils.dates_and_times import parse_collection_start_and_end
from utils.geodata import INSDC_GEO_LOC_NAMES, ABBREV_TO_US_STATE


class SraRunTableCsv(FileFormat):

    # todo:
    #  geo_loc_name_country and geo_loc_name_country_continent are currently just ignored

    @classmethod
    async def insert_from_file(cls, filename: str) -> None:
        debug_info = {
            'lines_skipped_preexisting': 0,
            'malformed_lines': 0
        }

        with open(filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=',')
            cls._verify_header(reader)
            for row in reader:
                try:
                    # parse geo location
                    geo_location_id = await cls._get_geo_loc_id(row)

                    # parse collection date
                    collection_start_date = collection_end_date = None
                    collection_date = get_value(
                        row,
                        cls.ColNameMapping.collection_date.value,
                        allow_none=False
                    )
                    if collection_date != 'missing':
                        collection_start_date, collection_end_date = parse_collection_start_and_end(collection_date)

                    # parse retraction date
                    retraction_detected_date = get_value(
                        row,
                        cls.ColNameMapping.retraction_detected_date.value,
                        allow_none=True
                    )
                    if retraction_detected_date is not None:
                        # todo: handle the tz better
                        retraction_detected_date = parser.isoparse(retraction_detected_date + 'Z')

                    sample = Sample(
                        geo_location_id=geo_location_id,
                        accession=get_value(row, cls.ColNameMapping.accession.value),
                        assay_type=get_value(row, cls.ColNameMapping.assay_type.value),
                        avg_spot_length=get_value(
                            row,
                            cls.ColNameMapping.avg_spot_length.value,
                            allow_none=True,
                            transform=float
                        ),
                        bases=get_value(row, cls.ColNameMapping.bases.value, transform=int),
                        bio_project=get_value(row, cls.ColNameMapping.bio_project.value),
                        bio_sample=get_value(row, cls.ColNameMapping.bio_sample.value, allow_none=True),
                        bio_sample_model=get_value(row, cls.ColNameMapping.bio_sample_model.value),
                        bio_sample_accession=get_value(
                            row,
                            cls.ColNameMapping.bio_sample_accession.value,
                            allow_none=True
                        ),
                        bytes=get_value(row, cls.ColNameMapping.bytes_.value, transform=int),
                        center_name=get_value(row, cls.ColNameMapping.center_name.value),
                        collection_start_date=collection_start_date,
                        collection_end_date=collection_end_date,
                        consent_level=get_value(row, cls.ColNameMapping.consent_level.value),
                        datastore_filetype=get_value(row, cls.ColNameMapping.datastore_filetype.value),
                        datastore_provider=get_value(row, cls.ColNameMapping.datastore_provider.value),
                        datastore_region=get_value(row, cls.ColNameMapping.datastore_region.value),
                        experiment=get_value(row, cls.ColNameMapping.experiment.value),
                        host=get_value(row, cls.ColNameMapping.host.value, allow_none=True),
                        instrument=get_value(row, cls.ColNameMapping.instrument.value),
                        isolate=get_value(row, cls.ColNameMapping.isolate.value, allow_none=True),
                        library_name=get_value(row, cls.ColNameMapping.library_name.value),
                        library_layout=get_value(row, cls.ColNameMapping.library_layout.value),
                        library_selection=get_value(row, cls.ColNameMapping.library_selection.value),
                        library_source=get_value(row, cls.ColNameMapping.library_source.value),
                        organism=get_value(row, cls.ColNameMapping.organism.value),
                        platform=get_value(row, cls.ColNameMapping.platform.value),
                        version=get_value(row, cls.ColNameMapping.version.value),
                        sample_name=get_value(row, cls.ColNameMapping.sample_name.value),
                        sra_study=get_value(row, cls.ColNameMapping.sra_study.value),
                        serotype=get_value(row, cls.ColNameMapping.serotype.value, allow_none=True),
                        isolation_source=get_value(row, cls.ColNameMapping.isolation_source.value, allow_none=True),
                        is_retracted=get_value(row, cls.ColNameMapping.is_retracted.value, transform=bool_from_str),
                        retraction_detected_date=retraction_detected_date,
                        release_date=get_value(row, cls.ColNameMapping.release_date.value, transform=parser.isoparse),
                        creation_date=get_value(row, cls.ColNameMapping.creation_date.value, transform=parser.isoparse),
                    )

                    _, preexisting = await find_or_insert_sample(sample)
                    if preexisting:
                        # todo: logging
                        debug_info['lines_skipped_preexisting'] += 1
                except ValueError as e:
                    # todo: logging
                    debug_info['malformed_lines'] += 1

        print(debug_info)

    class ColNameMapping(Enum):
        accession = 'Run'
        assay_type = 'Assay Type'
        avg_spot_length = 'AvgSpotLen'
        bases = 'Bases'
        bio_project = 'BioProject'
        bio_sample = 'BioSample'
        bio_sample_model = 'BioSampleModel'
        bytes_ = 'Bytes'
        center_name = 'Center Name'
        collection_date = 'Collection_Date'
        consent_level = 'Consent'
        datastore_filetype = 'DATASTORE filetype'
        datastore_provider = 'DATASTORE provider'
        datastore_region = 'DATASTORE region'
        experiment = 'Experiment'
        geo_loc_name_country = 'geo_loc_name_country'
        geo_loc_name_country_continent = 'geo_loc_name_country_continent'
        geo_loc_name = 'geo_loc_name'
        host = 'Host'
        instrument = 'Instrument'
        isolate = 'isolate'
        library_name = 'Library Name'
        library_layout = 'LibraryLayout'
        library_selection = 'LibrarySelection'
        library_source = 'LibrarySource'
        organism = 'Organism'
        platform = 'Platform'
        release_date = 'ReleaseDate'
        creation_date = 'create_date'
        version = 'version'
        sample_name = 'Sample Name'
        sra_study = 'SRA Study'
        serotype = 'serotype'
        isolation_source = 'isolation_source'
        bio_sample_accession = 'BioSample Accession'
        is_retracted = 'is_retracted'
        retraction_detected_date = 'retraction_detection_date_utc'

    @classmethod
    def _verify_header(cls, reader: DictReader):
        expected_header = [
            cls.ColNameMapping.accession.value,
            cls.ColNameMapping.assay_type.value,
            cls.ColNameMapping.avg_spot_length.value,
            cls.ColNameMapping.bases.value,
            cls.ColNameMapping.bio_project.value,
            cls.ColNameMapping.bio_sample.value,
            cls.ColNameMapping.bio_sample_model.value,
            cls.ColNameMapping.bytes_.value,
            cls.ColNameMapping.center_name.value,
            cls.ColNameMapping.collection_date.value,
            cls.ColNameMapping.consent_level.value,
            cls.ColNameMapping.datastore_filetype.value,
            cls.ColNameMapping.datastore_provider.value,
            cls.ColNameMapping.datastore_region.value,
            cls.ColNameMapping.experiment.value,
            cls.ColNameMapping.geo_loc_name_country.value,
            cls.ColNameMapping.geo_loc_name_country_continent.value,
            cls.ColNameMapping.geo_loc_name.value,
            cls.ColNameMapping.host.value,
            cls.ColNameMapping.instrument.value,
            cls.ColNameMapping.isolate.value,
            cls.ColNameMapping.library_name.value,
            cls.ColNameMapping.library_layout.value,
            cls.ColNameMapping.library_selection.value,
            cls.ColNameMapping.library_source.value,
            cls.ColNameMapping.organism.value,
            cls.ColNameMapping.platform.value,
            cls.ColNameMapping.release_date.value,
            cls.ColNameMapping.creation_date.value,
            cls.ColNameMapping.version.value,
            cls.ColNameMapping.sample_name.value,
            cls.ColNameMapping.sra_study.value,
            cls.ColNameMapping.serotype.value,
            cls.ColNameMapping.isolation_source.value,
            cls.ColNameMapping.bio_sample_accession.value,
            cls.ColNameMapping.is_retracted.value,
            cls.ColNameMapping.retraction_detected_date.value,
        ]
        if reader.fieldnames != expected_header:
            raise ValueError('did not find expected header')

    @classmethod
    async def _get_geo_loc_id(cls, row: Dict) -> int:
        geo_location_id = None
        geo_loc_full_text = get_value(row, cls.ColNameMapping.geo_loc_name.value, allow_none=True)
        if geo_loc_full_text is not None:
            country_name, region_name, locality_name = cls._parse_geo_loc(geo_loc_full_text)
            geo_loc = GeoLocation(
                full_text=geo_loc_full_text,
                country_name=country_name,
                region_name=region_name,
                locality_name=locality_name
            )
            geo_location_id = await find_or_insert_geo_location(geo_loc)
        return geo_location_id

    @classmethod
    def _parse_geo_loc(cls, geo_loc_name: str) -> (str, str, str):
        """
            Examples:
                USA
                USA: CA
                USA: Alaska
                USA: Plympton, MA
                USA: Minnesota, Kandiyohi County
                USA: Alaska, Matanuska-Susitna Borough
            :param text: geo_loc_name from the SRA
            :return: geo_loc_name (ie, country) , region, locality
            """
        # todo: deal with capitalization
        s_colon = geo_loc_name.split(':')
        # geo_loc_name
        gln = s_colon[0].strip()
        region = locality = None
        if gln not in INSDC_GEO_LOC_NAMES:
            raise ValueError('geo_loc_name should be from the approved list')
        if len(s_colon) > 1:
            s_comma = s_colon[1].split(',')
            region = s_comma[0].strip()
            if len(s_comma) > 1:
                locality = s_comma[1].strip()
                try:
                    # the second value should be locality, but some entries do 'city, CA'
                    region = ABBREV_TO_US_STATE[locality]
                    locality = s_comma[0].strip()
                except KeyError:
                    pass
            try:
                region = ABBREV_TO_US_STATE[region]
            except KeyError:
                pass
        return gln, region, locality
