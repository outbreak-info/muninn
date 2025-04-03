import argparse
import asyncio
import sys
from datetime import datetime
from os import path

from DB.inserts.file_formats.eve_dms_csv import EveDmsCsv
from DB.old_inserts import main as old_main
from DB.inserts.file_formats.file_format import FileFormat
from DB.inserts.file_formats.combined_tsv_v1 import CombinedTsvV1
from DB.inserts.file_formats.sra_run_table_csv import SraRunTableCsv


def main():
    # define allowed formats, give names and point to parsers
    formats = {
        'combined_tsv_v1': CombinedTsvV1,
        'sra_run_table_csv': SraRunTableCsv,
        'eve_dms_csv': EveDmsCsv,
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
    file_format: FileFormat = formats[args.format]

    # run inserts method
    start_time = datetime.now()
    print(f'{filename} {args.format} start at {start_time}')
    asyncio.run(file_format.insert_from_file(filename))
    end_time = datetime.now()
    print(f'{filename} {args.format} end at {end_time}, elapsed: {end_time - start_time}')


def mutations_kludge(basedir: str):
    print('Running the mutations kludge')
    asyncio.run(old_main(basedir))
    sys.exit(0)

if __name__ == '__main__':
    main()
