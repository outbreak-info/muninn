from csv import DictReader
from enum import Enum

from DB.inserts.file_formats.file_format import FileFormat
from utils.csv_helpers import get_value


# todo: this is a kludgy solution
class DmsCsvV1(FileFormat):

    @classmethod
    async def insert_from_file(cls, filename: str) -> None:
        with open(filename, 'r') as f:
            reader = DictReader(f, delimiter=',')
            cls._verify_header(reader)

            for row in reader:

                # get aa data
                # todo: kludge
                gff_feature = 'HA:cds-XAJ25415.1'

                ref_aa = get_value(row, cls.ColNameMapping.ref_aa.value)
                alt_aa = get_value(row, cls.ColNameMapping.alt_aa.value)
                position_aa = get_value(row, cls.ColNameMapping.position_aa.value, transform=int)



    class ColNameMapping(Enum):
        # cols we use
        ref_aa = 'ref'
        alt_aa = 'mutant'
        position_aa = 'pos'
        species_sera_escape = 'species sera escape'
        entry_in_293t_cells = 'entry in 293T cells'
        stability = 'stability'
        sa26_usage_increase = 'SA26 usage increase'
        sequential_site = 'sequential_site'
        ref_h1_site = 'reference_H1_site'
        mature_h5_site = 'mature_H5_site'
        nt_changes_to_codon = 'nt changes to codon'
        ferret_sera_escape = 'ferret sera escape'
        mouse_sera_escape = 'mouse sera escape'

        # cols we ignore for now
        # some we don't need
        consensus = 'Consensus'
        accession = 'sra'
        region = 'region'
        # todo: and some won't fit in a float at all
        antibody_set = 'antibody_set'
        ha1_ha2_h5_site = 'HA1_HA2_H5_site'


    @classmethod
    def _verify_header(cls, reader: DictReader) -> None:
        expected_header = [
            cls.ColNameMapping.consensus.value,
            cls.ColNameMapping.ref_aa.value,
            cls.ColNameMapping.position_aa.value,
            cls.ColNameMapping.alt_aa.value,
            cls.ColNameMapping.accession.value,
            cls.ColNameMapping.species_sera_escape.value,
            cls.ColNameMapping.entry_in_293t_cells.value,
            cls.ColNameMapping.stability.value,
            cls.ColNameMapping.sa26_usage_increase.value,
            cls.ColNameMapping.sequential_site.value,
            cls.ColNameMapping.ref_h1_site.value,
            cls.ColNameMapping.mature_h5_site.value,
            cls.ColNameMapping.ha1_ha2_h5_site.value,
            cls.ColNameMapping.region.value,
            cls.ColNameMapping.nt_changes_to_codon.value,
            cls.ColNameMapping.antibody_set.value,
            cls.ColNameMapping.ferret_sera_escape.value,
            cls.ColNameMapping.mouse_sera_escape.value,
        ]

        if reader.fieldnames != expected_header:
            raise ValueError('did not find expected header')