class OneTimeDict(dict):
    def __setitem__(self, key, value):
        if key in self:
            raise KeyError('{} has already been set'.format(key))
        super().__setitem__(key, value)
