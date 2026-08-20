import argparse
import asyncio
from enum import StrEnum

from DB.structure import cache_cns_pmv_sums


class Commands(StrEnum):
    create = 'create'


def main():
    argparser = argparse.ArgumentParser(description='Muninn cache management')
    argparser.add_argument('command', help=f'Options: {", ".join(Commands)}')

    args = argparser.parse_args()

    match args.command:
        case Commands.create:
            create_caches()
        case _:
            raise ValueError(f'Not a recognized command: {args.command}')


def create_caches():
    asyncio.run(cache_cns_pmv_sums.create_all())


if __name__ == '__main__':
    main()
