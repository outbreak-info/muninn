from abc import ABC, abstractmethod

'''
Some notes about what can be in this file:

What is it that all the insertion systems will share?
- They will read records from a file
- They have data that they mandate must be in each record.
--> could those tasks be lifted up into this class?
'''

class FileParser(ABC):

    @abstractmethod
    def __init__(self, filename: str):
        raise NotImplementedError

    @abstractmethod
    async def parse_and_insert(self):
        raise NotImplementedError
