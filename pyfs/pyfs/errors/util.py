
import errno

from .errors import *

EXCEPTION_MAPPING = {
    errno.EEXIST    : FileExistsError,
    errno.ENOENT    : FileNotFoundError,
    errno.EPERM     : PermissionError,
    errno.ENOTEMPTY : DirectoryNotEmptyError,
    errno.ENOTDIR   : NotADirectoryError,
    errno.EISDIR    : IsADirectoryError,
    errno.EAGAIN    : PathIsLockedError,
}

REVERSE_EXCEPTION_MAPPING = {}
for code in EXCEPTION_MAPPING:
    REVERSE_EXCEPTION_MAPPING[EXCEPTION_MAPPING[code]] = code


def remap(e):
    # todo: add more exceptions for other OSErrors, like permissions, etc.
    if e.errno in EXCEPTION_MAPPING:
        return EXCEPTION_MAPPING[e.errno](e.errno, e.strerror, e.filename)
    return e


def trap(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except (IOError, OSError) as e:
        raise remap(e)
