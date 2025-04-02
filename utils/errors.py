class ParsingError(Exception):


    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'ParsingError: {self.message}'


class NotFoundError(Exception):
    """
    Used to represent the case in which results are expected but are not found.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'NotFoundError: {self.message}'
