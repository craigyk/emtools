from __future__ import absolute_import

import imp

from .glob import glob
from .cmds import join, rext, bname


def loader(path):
    return Plugins(path)


class Plugins(object):

    def __init__(self, path):
        self.__path = path

    def __reload(self):
        loaded = {}
        for path in glob(join(self.__path, "*.py")):
            module_name = rext(bname(path))
            module = imp.load_source(module_name, path)
            for ext in getattr(module, "NAMES", []):
                setattr(self, ext, module)
                loaded[ext] = module
        return loaded

    def __getattr__(self, key):
        return self.__reload()[key]

    def __getitem__(self, key):
        return getattr(self, key)

    def __contains__(self, key):
        if not isinstance(key, str):
            return False
        return hasattr(self, key)
