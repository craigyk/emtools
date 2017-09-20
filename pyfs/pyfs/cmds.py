from __future__ import absolute_import

import os
import shutil
from contextlib import contextmanager

from .errors import trap
from .errors.errors import *


def rm(path):
    try:
        trap(os.unlink, path)
    except FileNotFoundError:
        pass
    return path


def rmdir(path):
    try:
        trap(os.rmdir, path)
    except FileNotFoundError:
        pass
    return path


def rmdirs(path, stop=None):
    while path != stop:
        try:
            rmdir(path)
        except DirectoryNotEmptyError:
            break
        path = dpath(path)
    return path


def isdir(path):
    return trap(os.path.isdir, path) and not islns(path)


def isfile(path):
    return trap(os.path.isfile, path) and not islns(path)


def islns(path):
    return trap(os.path.islink, path)


def mkdir(path):
    if not isdir(path):
        trap(os.mkdir, path)
    return path


def mkdirs(path):
    if isdir(path):
        return path
    return trap(os.makedirs, path)


def ls(path):
    try:
        return trap(os.listdir, path)
    except FileNotFoundError:
        return []


def rext(path, new_ext=None, full=False):
    path, last = os.path.splitext(path)
    while full and last:
        path, last = os.path.splitext(path)
    if new_ext not in (None, "", False):
        return "%s.%s" % (path, new_ext)
    return path


def gext(path, last=False):
    parts = bname(path).split('.')
    if len(parts) == 1:
        return None
    elif last:
        return parts[-1]
    return ".".join(parts[1:])


def sext(path, full=False):
    path, ext = os.path.splitext(path)
    lext = [ext[1:]]
    while full and ext:
        path, ext = os.path.splitext(path)
        lext = [ext[1:]] + lext
    lext = [e for e in lext if e != '']
    return path, ".".join(lext)


def samefile(srcpath, dstpath):
    if srcpath == dstpath:
        return True
    try:
        stat1 = lstat(srcpath)
        stat2 = lstat(dstpath)
    except FileNotFoundError:
        return False
    return (stat1.st_dev, stat1.st_ino) == (stat2.st_dev, stat2.st_ino)


def samelink(srcpath, dstpath):
    if srcpath == dstpath:
        return True
    return ( resolve(srcpath) == resolve(dstpath) ) or samefile(srcpath, dstpath)


def ln(srcpath, dstpath):
    if not samefile(srcpath, dstpath):
        with shadow(dstpath):
            trap(os.link, srcpath, dstpath)
    return dstpath


def lns(srcpath, dstpath):
    if not samelink(srcpath, dstpath):
        with shadow(dstpath):
            trap(os.symlink, srcpath, dstpath)
    return dstpath


def stat(path):
    return trap(os.stat, path)


def lstat(path):
    return trap(os.lstat, path)


def utime(path):
    return trap(os.utime, path)


exists = os.path.exists
relative = os.path.relpath
resolve = os.path.abspath
dpath = os.path.dirname
bname = os.path.basename


def common(*paths):
    return os.path.commonprefix(list(paths))


def mtime(path):
    return trap(stat, path).st_mtime


def rmtree(root):
    for fname in ls(root):
        path = join(root, fname)
        if os.path.islink(path):
            rm(path)
        elif os.path.isdir(path):
            rmtree(path)
        elif os.path.isfile(path):
            rm(path)
    rmdir(root)


def join(*comps):
    root = ""
    for comp in comps:
        if comp:
            root = os.path.join(root, comp)
    return os.path.normpath(root)


def cp(srcpath, dstpath):
    if not samefile(srcpath, dstpath):
        with shadow(dstpath):
            trap(shutil.copy, srcpath, dstpath)
    return dstpath


def mv(srcpath, dstpath):
    with shadow(dstpath):
        trap(os.rename, srcpath, dstpath)
    return dstpath


def split(path):
    parts = path.split(os.path.sep)
    parts = [x for x in parts if x != ""]
    if path[0] == "/":
        parts = ["/"] + parts
    return parts


def find_break(path):
    root = "/"
    for component in split(resolve(path)):
        _root = join(root, component)
        if not exists(_root):
            break
        root = _root
    return root


def touch(path, time=None):
    with shadow(path):
        with file(path, 'a'):
            os.utime(path, time)
    return path





@contextmanager
def shadow(path):
    src = dpath(resolve(path))
    dst = find_break(src)
    try:
        yield mkdirs(src)
    except FileExistsError:
        yield path
    finally:
        rmdirs(src, dst)


class Shadow(object):
    # context manager for creating a path tree, and cleaning it up
    # if neccessary
    def __init__(self, path):
        self.path = dpath(resolve(path))
        self.stop = find_break(self.path)

    def __enter__(self):
        mkdirs(self.path)
        return self

    def __exit__(self, ev, ex, et):
        self.restore()
        return False

    def restore(self):
        try:
            rmdirs(self.path, self.stop)
        except DirectoryNotEmptyError:
            pass
        return self


MODES = {
    "b": getattr(os, "O_BINARY", 0),
    "c": os.O_CREAT | os.O_EXCL,
    "a": os.O_CREAT | os.O_WRONLY | os.O_APPEND,
    "w": os.O_CREAT | os.O_WRONLY | os.O_TRUNC,
    "n": getattr(os, "O_NONBLOCK", 0),
    "s": getattr(os, "O_SHLOCK", 0),
    "x": getattr(os, "O_EXLOCK", 0),
    "r": os.O_RDONLY,
    "+": os.O_RDWR,
}


def mode_to_flags(mode):
    flags = 0
    for mod in MODES:
        if mod in mode:
            flags |= MODES[mod]
    if ( flags & os.O_RDWR ):
        flags &= ~(os.O_RDONLY | os.O_WRONLY)
    return flags


def _open(path, flags=0, stat=0o755):
    return trap(os.open, path, flags, stat)


def _fdopen(fd, mode="rb"):
    return trap(os.fdopen, fd, mode)


def open(path, mode="rb", stat=0o755):
    # adds the following options to mode
    # 'c': create file, exception if file already exists
    # 's': acquire a shared lock on the file
    # 'x': acquire an exclusive lock on file
    # 'n': do not block on locks, used in conjunction with s or x
    flags = mode_to_flags(mode)
    mode = mode.replace('c', '')
    return _fdopen(_open(path, flags, stat), mode)


def shlock(path):
    return open(path, "rbs")


def exlock(path):
    return open(path, "rbx")


def read(path, mode="rb"):
    with open(path, mode) as src:
        return src.read()


def write(path, data, mode="wb"):
    with open(path, mode) as dst:
        dst.write(data)


def create(path):
    with shadow(path):
        with open(path, "ac"):
            pass
    return path


@contextmanager
def temp(path):
    try:
        yield path
    finally:
        rm(path)
