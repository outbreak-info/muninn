import asyncio

from DB.structure.mutations import drop_existing_mutations, create_bitmap_mutations_table


async def main():
    await drop_existing_mutations()
    await create_bitmap_mutations_table()


if __name__ == '__main__':
    asyncio.run(main())