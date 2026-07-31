import asyncio

from DB.structure import runner


def main():
    asyncio.run(runner.set_up_db())


if __name__ == '__main__':
    main()
