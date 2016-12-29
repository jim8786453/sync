
from sync.constants import Text


class SyncError(Exception):
    """Base Sync error."""
    pass


class DatabaseNotFoundError(SyncError):
    """The database for a network could not be found."""
    pass


class InvalidIdError(SyncError):
    """The value provided was not a valid UUID."""
    pass


class InvalidOperationError(SyncError):
    """The operation can not be performed."""
    pass


class NotFoundError(SyncError):
    """The object could not be found in storage."""

    def __init__(self, type_, id_):
        """Initialise the error.

        :param type_: The object type.
        :type type_: str
        :param type_: The object id.
        :type id: str

        """
        self.type = type_
        self.id = id_
        text = Text.NotFound.format(self.type, self.id)
        super(SyncError, self).__init__(text)


class InvalidJsonError(SyncError):
    """JSON is not valid."""
    pass
