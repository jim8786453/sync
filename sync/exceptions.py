
from sync.constants import Text


class SyncError(Exception):
    pass


class DatabaseNotFoundError(SyncError):
    pass


class InvalidIdError(SyncError):
    pass


class InvalidOperationError(SyncError):
    pass


class NotFoundError(SyncError):

    def __init__(self, type_, id_):
        self.type = type_
        self.id = id_
        text = Text.NotFound.format(self.type, self.id)
        super(SyncError, self).__init__(text)
