from DB.inserts.file_formats.file_format import FileFormat

# todo: this is a kludgy solution
class DmsCsvV1(FileFormat):

    @classmethod
    async def insert_from_file(cls, filename: str) -> None:
        pass