import csv
from csv import DictReader
from enum import Enum

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.geo_locations import find_or_insert_geo_location
from DB.inserts.samples import find_or_insert_sample
from DB.models import GeoLocation, Sample
from utils.csv_helpers import get_value, bool_from_str
from utils.dates_and_times import parse_collection_start_and_end
from utils.geodata import parse_geo_loc


class SamplesTsvParser(FileParser):

    def __init__(self, filename: str):
        self.filename = filename

    async def parse_and_insert(self, dateutil=None):
        debug_info = {
            'skipped_malformed': 0
        }

        # (country, region, locality) -> id
        cache_geo_loc_ids = dict()

        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=',')
            SamplesTsvParser._verify_header(reader)
            for row in reader:
                try:
                    # parse geo location
                    geo_location_id = None
                    geo_loc_full_text = get_value(
                        row,
                        SamplesTsvParser.ColNameMapping.geo_loc_name.value,
                        allow_none=True
                    )
                    if geo_loc_full_text is not None:
                        country_name, region_name, locality_name = parse_geo_loc(geo_loc_full_text)
                        try:
                            geo_location_id = cache_geo_loc_ids[(country_name, region_name, locality_name)]
                        except KeyError:
                            geo_loc = GeoLocation(
                                full_text=geo_loc_full_text,
                                country_name=country_name,
                                region_name=region_name,
                                locality_name=locality_name
                            )
                            geo_location_id = await find_or_insert_geo_location(geo_loc)
                            cache_geo_loc_ids[(country_name, region_name, locality_name)] = geo_location_id

                    # parse collection date
                    collection_start_date = collection_end_date = None
                    collection_date = get_value(
                        row,
                        SamplesTsvParser.ColNameMapping.collection_date.value,
                        allow_none=False
                    )
                    if collection_date != 'missing':
                        collection_start_date, collection_end_date = parse_collection_start_and_end(collection_date)

                    # parse retraction date
                    retraction_detected_date = get_value(
                        row,
                        SamplesTsvParser.ColNameMapping.retraction_detected_date.value,
                        allow_none=True
                    )
                    if retraction_detected_date is not None:
                        # todo: handle the tz better
                        retraction_detected_date = dateutil.parser.isoparse(retraction_detected_date + 'Z')

                    sample = Sample(
                        geo_location_id=geo_location_id,
                        accession=get_value(row, SamplesTsvParser.ColNameMapping.accession.value),
                        assay_type=get_value(row, SamplesTsvParser.ColNameMapping.assay_type.value),
                        avg_spot_length=get_value(
                            row,
                            SamplesTsvParser.ColNameMapping.avg_spot_length.value,
                            allow_none=True,
                            transform=float
                        ),
                        bases=get_value(row, SamplesTsvParser.ColNameMapping.bases.value, transform=int),
                        bio_project=get_value(row, SamplesTsvParser.ColNameMapping.bio_project.value),
                        bio_sample=get_value(row, SamplesTsvParser.ColNameMapping.bio_sample.value, allow_none=True),
                        bio_sample_model=get_value(row, SamplesTsvParser.ColNameMapping.bio_sample_model.value),
                        bio_sample_accession=get_value(
                            row,
                            SamplesTsvParser.ColNameMapping.bio_sample_accession.value,
                            allow_none=True
                        ),
                        bytes=get_value(row, SamplesTsvParser.ColNameMapping.bytes_.value, transform=int),
                        center_name=get_value(row, SamplesTsvParser.ColNameMapping.center_name.value),
                        collection_start_date=collection_start_date,
                        collection_end_date=collection_end_date,
                        consent_level=get_value(row, SamplesTsvParser.ColNameMapping.consent_level.value),
                        datastore_filetype=get_value(row, SamplesTsvParser.ColNameMapping.datastore_filetype.value),
                        datastore_provider=get_value(row, SamplesTsvParser.ColNameMapping.datastore_provider.value),
                        datastore_region=get_value(row, SamplesTsvParser.ColNameMapping.datastore_region.value),
                        experiment=get_value(row, SamplesTsvParser.ColNameMapping.experiment.value),
                        host=get_value(row, SamplesTsvParser.ColNameMapping.host.value, allow_none=True),
                        instrument=get_value(row, SamplesTsvParser.ColNameMapping.instrument.value),
                        isolate=get_value(row, SamplesTsvParser.ColNameMapping.isolate.value, allow_none=True),
                        library_name=get_value(row, SamplesTsvParser.ColNameMapping.library_name.value),
                        library_layout=get_value(row, SamplesTsvParser.ColNameMapping.library_layout.value),
                        library_selection=get_value(row, SamplesTsvParser.ColNameMapping.library_selection.value),
                        library_source=get_value(row, SamplesTsvParser.ColNameMapping.library_source.value),
                        organism=get_value(row, SamplesTsvParser.ColNameMapping.organism.value),
                        platform=get_value(row, SamplesTsvParser.ColNameMapping.platform.value),
                        version=get_value(row, SamplesTsvParser.ColNameMapping.version.value),
                        sample_name=get_value(row, SamplesTsvParser.ColNameMapping.sample_name.value),
                        sra_study=get_value(row, SamplesTsvParser.ColNameMapping.sra_study.value),
                        serotype=get_value(row, SamplesTsvParser.ColNameMapping.serotype.value, allow_none=True),
                        isolation_source=get_value(
                            row,
                            SamplesTsvParser.ColNameMapping.isolation_source.value,
                            allow_none=True
                        ),
                        is_retracted=get_value(
                            row,
                            SamplesTsvParser.ColNameMapping.is_retracted.value,
                            transform=bool_from_str
                        ),
                        retraction_detected_date=retraction_detected_date,
                        release_date=get_value(
                            row,
                            SamplesTsvParser.ColNameMapping.release_date.value,
                            transform=dateutil.parser.isoparse
                        ),
                        creation_date=get_value(
                            row,
                            SamplesTsvParser.ColNameMapping.creation_date.value,
                            transform=dateutil.parser.isoparse
                        ),
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
        required_columns = {cn.value for cn in {
            cls.ColNameMapping.accession,
            cls.ColNameMapping.assay_type,
            cls.ColNameMapping.avg_spot_length,
            cls.ColNameMapping.bases,
            cls.ColNameMapping.bio_project,
            cls.ColNameMapping.bio_sample,
            cls.ColNameMapping.bio_sample_model,
            cls.ColNameMapping.bytes_,
            cls.ColNameMapping.center_name,
            cls.ColNameMapping.collection_date,
            cls.ColNameMapping.consent_level,
            cls.ColNameMapping.datastore_filetype,
            cls.ColNameMapping.datastore_provider,
            cls.ColNameMapping.datastore_region,
            cls.ColNameMapping.experiment,
            cls.ColNameMapping.geo_loc_name_country,
            cls.ColNameMapping.geo_loc_name_country_continent,
            cls.ColNameMapping.geo_loc_name,
            cls.ColNameMapping.host,
            cls.ColNameMapping.instrument,
            cls.ColNameMapping.isolate,
            cls.ColNameMapping.library_name,
            cls.ColNameMapping.library_layout,
            cls.ColNameMapping.library_selection,
            cls.ColNameMapping.library_source,
            cls.ColNameMapping.organism,
            cls.ColNameMapping.platform,
            cls.ColNameMapping.release_date,
            cls.ColNameMapping.creation_date,
            cls.ColNameMapping.version,
            cls.ColNameMapping.sample_name,
            cls.ColNameMapping.sra_study,
            cls.ColNameMapping.serotype,
            cls.ColNameMapping.isolation_source,
            cls.ColNameMapping.bio_sample_accession,
            cls.ColNameMapping.is_retracted,
            cls.ColNameMapping.retraction_detected_date,
        }}
        if not set(reader.fieldnames) >= required_columns:
            raise ValueError(f'Missing required columns: {required_columns - set(reader.fieldnames)}')
