

import sys


class MappedOSError(OSError):

    def __str__(self):
        return "%s: %s" % (self.strerror, self.filename)

    def __repr__(self):
        return self.__str__()


class TimeoutError(MappedOSError):
    pass


class PathAlreadyLockedError(MappedOSError):
    pass


class DirectoryNotEmptyError(MappedOSError):
    pass


class PathIsLockedError(MappedOSError):
    pass


if sys.version_info.major == 2:

    class PermissionError(MappedOSError):
        pass

    class FileExistsError(MappedOSError):
        pass

    class FileNotFoundError(MappedOSError):
        pass

    class IsADirectoryError(MappedOSError):
        pass

    class NotADirectoryError(MappedOSError):
        pass

else:

    PermissionError = PermissionError
    FileExistsError = FileExistsError
    FileNotFoundError = FileNotFoundError
    IsADirectoryError = IsADirectoryError
    NotADirectoryError = NotADirectoryError
