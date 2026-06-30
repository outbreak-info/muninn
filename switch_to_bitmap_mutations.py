import asyncio

from DB.structure.mutation_translations import drop_existing_mutation_translations, \
    create_bitmap_mutation_translations_table
from DB.structure.mutations import drop_existing_mutations, create_bitmap_mutations_table


async def main():
    await drop_existing_mutations()
    await create_bitmap_mutations_table()
    await drop_existing_mutation_translations()
    await create_bitmap_mutation_translations_table()


if __name__ == '__main__':
    asyncio.run(main())