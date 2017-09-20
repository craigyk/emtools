from __future__ import absolute_import

import json
import yaml
import pickle

from oneup import defer

from .aopen import aopen
from .errors import *


etypes = {
    'yaml'   : yaml,
    'json'   : json,
    'pickle' : pickle
}


def store(path, etype="pickle"):
    return FSStore(path, etype)


class FSStore(object):

    '''
    somewhat similar to the python shelve object, except
    that it uses atomic updates to prevent other readers
    from reading updates in progress
    '''

    def __init__(self, path=None, etype='python', lock=False):
        self.etype = etype
        self.paused = False
        self.dirty = False
        self.path = path
        self.dict = {}
        self.load()

    @defer
    def _load(self):
        module = __import__(self.etype)
        return module.load

    @defer
    def _dump(self):
        module = __import__(self.etype)
        return module.dump

    def __setitem__(self, key, value):
        self.dirty = True
        self.dict[key] = value
        if not self.paused:
            self.dump()
        return value

    def __contains__(self, key):
        return key in self.dict

    def get(self, key, default):
        return self.dict.get(key, default)

    def load(self):
        if self.path:
            try:
                with aopen(self.path, 'rb') as src:
                    self.dict.update(self._load(src))
            except FileNotFoundError:
                pass

    def dump(self):
        if self.dirty and self.path:
            with aopen(self.path, 'wb') as dst:
                self._dump(self.dict, dst)
                self.dirty = False

    def update(self, values):
        self.dict.update(values)
        self.dump()

    def __repr__(self):
        return dict.__repr__(self.dict)

    def __str__(self):
        return dict.__str__(self.dict)

    def __enter__(self):
        self.paused = True
        return self

    def __exit__(self, ev, et, ex):
        self.paused = False
        if not ev:
            self.dump()

