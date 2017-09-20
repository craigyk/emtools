
import pyfs
from contextlib import contextmanager


def jail(root):
    return Jail(root)


@contextmanager
def mkdtemp(*args):
    import tempfile
    jail = Jail(tempfile.mkdtemp(*args))
    yield jail
    jail.rmtree()


class Jail(object):

    '''
    provides an advisory filesystem "jail" that helps limit path operations to a specified root
    directory.  It supplies method for acquiring atomic locks on paths, and
    for opening files for write and updates in an psuedo-atomic fashion.  All of this
    is to try and make it safer for files to be read, written and updated from multiple
    processes
    '''

    def __init__(self, path):
        self.base = path

    def join(self, path=None):
        if path and path.startswith(self.base):
            return path
        path = pyfs.join(self.base, path)
        if self.base not in path:
            # if the path we are creating is not in the cache
            # or on the way to it then we have a problem
            raise ValueError("path: %s is outside jail: %s" % (path, self.base))
        return path

    def touch(self, path):
        pyfs.touch(self.join(path))

    def ext(self, ext):
        return self.base + ext

    def open(self, path, mode, perm=0x755):
        return pyfs.aopen(self.join(path), mode, perm)

    def exists(self, path=None):
        return pyfs.exists(self.join(path))

    def rm(self, path=None):
        return pyfs.rm(self.join(path))

    def rmdir(self, path=None):
        return pyfs.rmdir(self.join(path))

    def rmtree(self, path=None):
        return pyfs.rmtree(self.join(path))

    def ln(self, srcpath, dstpath):
        return pyfs.ln(srcpath, self.join(dstpath))

    def mkdir(self, path=None):
        return pyfs.mkdir(self.join(path))

    def cp(self, src, dst):
        return pyfs.cp(src, self.join(dst))

    def mv(self, src, dst):
        return pyfs.mv(src, self.join(dst))

    def lock(self, path=None):
        return pyfs.lock(self.join(path))

    def locked(self, path=None):
        print("checking lock for:", self.path)
        return self.lock(path).locked

    def glob(self, pattern):
        return pyfs.glob(self.join(pattern))

    def ls(self, path=None):
        return pyfs.ls(self.join(path))

    def __enter__(self):
        self.ctxlock = self.lock()
        self.ctxlock.acquire()
        return self

    def __exit__(self, ev, et, es):
        self.ctxlock.release()
