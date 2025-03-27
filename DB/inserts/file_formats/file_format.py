from abc import ABC, abstractmethod


class FileFormat(ABC):

    @classmethod
    @abstractmethod
    async def insert_from_file(cls, filename: str) -> None:
        raise NotImplementedError
