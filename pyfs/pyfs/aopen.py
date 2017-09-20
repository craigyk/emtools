from __future__ import absolute_import

# tries to be the granddaddy of all open functions
# combines the extra flags of open in cmds.py with
# atomic-like operations that work by using side files
# and locks

from contextlib import contextmanager

from .lock import lock
from .cmds import cp, create, open, temp
from .errors.errors import *


def must_copy(mode):
    if "+" in mode:
        return True
    elif "a" in mode:
        return True
    elif "w" in mode:
        return True
    elif "c" in mode:
        return True
    return False


def must_make(mode):
    if "c" in mode:
        return True
    return False


@contextmanager
def aopen(path, mode="rb", stat=0o755):
    if must_copy(mode):
        with lock(path):
            if must_make(mode):
                create(path)
            with temp("%s.updating" % path) as tmppath:
                try:
                    cp(path, tmppath)
                except FileNotFoundError:
                    pass
                with open(tmppath, mode, stat) as file:
                    yield file
                cp(tmppath, path)
    else:
        yield open(path, mode, stat)


def aread(path, mode="rb"):
    with aopen(path, mode) as src:
        return src.read()


def awrite(path, data, mode="wb"):
    with aopen(path, mode) as dst:
        dst.write(data)
