import argparse
import asyncio
import sys
from datetime import datetime
from os import path
from typing import Any

from DB.inserts.file_parsers.dms_parser import HaRegionDmsTsvParser, HaRegionDmsCsvParser
from DB.inserts.file_parsers.eve_parser import EveCsvParser
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.file_parsers.freyja_demixed_parser import FreyjaDemixedParser
from DB.inserts.file_parsers.genoflu_lineages_parser import GenofluLineagesParser
from DB.inserts.file_parsers.mutations_parser import MutationsTsvParser
from DB.inserts.file_parsers.samples_parser import SamplesCsvParser, SamplesTsvParser
from DB.inserts.file_parsers.sarscov2_parsers.sc2_samples_parser import SC2SamplesParser
from DB.inserts.file_parsers.variants_mutations_combined_parser import VariantsMutationsCombinedParser
from DB.inserts.file_parsers.variants_tsv_parser import VariantsTsvParser


def main():
    # define allowed formats, give names and point to parsers
    formats = {
        'samples_csv': SamplesCsvParser,
        'samples_tsv': SamplesTsvParser,
        'variants_tsv': VariantsTsvParser,
        'eve_dms_csv': EveCsvParser,
        'genoflu_lineages': GenofluLineagesParser,
        'mutations_tsv': MutationsTsvParser,
        'ha_dms_tsv': HaRegionDmsTsvParser,
        'ha_dms_csv': HaRegionDmsCsvParser,
        'freyja_demixed': FreyjaDemixedParser,
        'variants_mutations_combined_tsv': VariantsMutationsCombinedParser,
        'sc2_samples': SC2SamplesParser
    }

    ## Parse and verify args ##
    argparser = argparse.ArgumentParser(
        description='Muninn Data Insertion'
    )
    argparser.add_argument('filenames', help='path to file to be parsed', nargs='*')
    argparser.add_argument(
        '--format',
        help=f"Name of the format to be parsed. Available formats are: {', '.join(formats.keys())}",
        nargs='?'
    )

    argparser.add_argument(
        '--req_cols',
        help='Print required column info for each format and exit',
        action='store_true',
        required=False
    )

    args = argparser.parse_args()

    if args.req_cols:
        print_req_col_info(formats)
        return
    elif args.filenames is None or args.format is None:
        print('Specify either a help option or a filename and format')
        argparser.print_help()
        return

    if not args.format in formats.keys():
        print(f'Invalid format name given: {args.format}')
        argparser.print_help()
        sys.exit(1)

    for f in args.filenames:
        if not path.exists(f):
            print(f'Input file or dir not found: {f}')
            argparser.print_help()
            sys.exit(1)

    file_parser: FileParser = formats[args.format]
    filename: str = args.filenames[0]
    filename2: str | None = None
    if issubclass(file_parser, VariantsMutationsCombinedParser):
        filename2 = args.filenames[1]
    elif len(args.filenames) > 1:
        raise ValueError('Multiple filenames provided, but this format takes only one.')

    # run inserts method
    start_time = datetime.now()
    print(f'{filename} {args.format} start at {start_time}')
    if issubclass(file_parser, FileParser):
        if issubclass(file_parser, VariantsMutationsCombinedParser):
            parser = file_parser(filename, filename2)
        else:
            parser = file_parser(filename)
        asyncio.run(parser.parse_and_insert())
    end_time = datetime.now()
    print(f'{filename} {args.format} end at {end_time}, elapsed: {end_time - start_time}')


def print_req_col_info(formats: dict[str, Any]) -> None:
    for name, parser in formats.items():
        if issubclass(parser, FileParser):
            print(name)
            for col in parser.get_required_column_set():
                print(f'\t{col}')


if __name__ == '__main__':
    main()
