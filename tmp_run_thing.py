import asyncio

from DB.inserts.file_parsers.variants_mutations_combined_parser import VariantsMutationsCombinedParser


def main():
    vmparser = VariantsMutationsCombinedParser(
        '/Users/james/Documents/muninn/test_data/playset_april/variants.tsv',
        '/Users/james/Documents/muninn/test_data/mutations_full_2025-05-28.tsv'
    )

    asyncio.run(vmparser.parse_and_insert())


if __name__ == '__main__':
    main()