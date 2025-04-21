import argparse
import asyncio
import sys
from datetime import datetime
from os import path

from DB.inserts.file_formats.eve_dms_csv import EveDmsCsv
from DB.inserts.file_formats.file_format import FileFormat
from DB.inserts.file_formats.sra_run_table_csv import SraRunTableCsv
from DB.inserts.file_formats.tmp_mouse_ferret_dms_tsv import TempHaMouseFerretDmsTsv
from DB.inserts.file_parsers.dms_parser import HaRegionDmsTsvParser
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.file_parsers.genoflu_lineages_parser import GenofluLineagesParser
from DB.inserts.file_parsers.mutations_parser import MutationsTsvParser
from DB.inserts.file_parsers.samples_parser import SamplesCsvParser, SamplesTsvParser
from DB.inserts.file_parsers.variants_tsv_parser import VariantsTsvParser
from DB.old_inserts import main as old_main


def main():
    # define allowed formats, give names and point to parsers
    formats = {
        'samples_csv': SamplesCsvParser,
        'samples_tsv': SamplesTsvParser,
        'variants_tsv': VariantsTsvParser,
        'sra_run_table_csv': SraRunTableCsv,
        'eve_dms_csv': EveDmsCsv,
        'tmp_ha_mouse_ferret_dms': TempHaMouseFerretDmsTsv,
        'genoflu_lineages': GenofluLineagesParser,
        'mutations_tsv': MutationsTsvParser,
        'ha_dms_tsv': HaRegionDmsTsvParser,
    }

    ## Parse and verify args ##
    parser = argparse.ArgumentParser(
        description='Muninn Data Insertion'
    )
    parser.add_argument('filename', help='path to file to be parsed')
    parser.add_argument(
        'format',
        help=f"Name of the format to be parsed. Available formats are: {','.join(formats.keys())}"
    )
    parser.add_argument(
        '--kludge_mutations',
        action='store_true',
        help='run the kludge script to insert mutations'
    )
    args = parser.parse_args()

    if args.kludge_mutations:
        mutations_kludge(args.filename)

    if not args.format in formats.keys():
        print('Invalid format name given')
        parser.print_help()
        sys.exit(1)

    if not path.isfile(args.filename):
        print('Input file not found')
        parser.print_help()
        sys.exit(1)

    filename = args.filename

    file_format: FileFormat | FileParser = formats[args.format]

    # run inserts method
    start_time = datetime.now()
    print(f'{filename} {args.format} start at {start_time}')
    if issubclass(file_format, FileParser):
        parser = file_format(filename)
        asyncio.run(parser.parse_and_insert())
    else:
        asyncio.run(file_format.insert_from_file(filename))
    end_time = datetime.now()
    print(f'{filename} {args.format} end at {end_time}, elapsed: {end_time - start_time}')


def mutations_kludge(basedir: str):
    print('Running the mutations kludge')
    asyncio.run(old_main(basedir))
    sys.exit(0)

if __name__ == '__main__':
    main()
