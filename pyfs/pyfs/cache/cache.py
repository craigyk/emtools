#!/usr/bin/env python

import time
import hashlib
import pickle

import pyfs
from oneup import defer

CACHE_ROOT = "CACHE"


def hashargs(args=[], kwargs={}, split=8):
    '''
    (a) -> {b:c} -> int -> str
    given a set of positional and keyword function arguments
    provide a deterministic FS path for hashing.  Uses md5.
    All keys and values must be convertible to string form.
    Currently splits the path into 8 characters to reduce number
    of possible entries in any given directory.
    '''
    md5 = hashlib.md5()
    for arg in args:
        md5.update(str(arg))
    for key in sorted(kwargs):
        md5.update(str(key))
        md5.update(str(kwargs[key]))
    hexdigest = md5.hexdigest()
    components = split_in(hexdigest, split)
    return pyfs.join(*components)


def split_in(string, count):
    '''
    str -> [str]
    splits a string into `count` number of parts
    '''
    return tuple([string[s:s+count] for s in range(0, len(string), count)])


def cached(func):
    '''
    used as a function decorator to convert a function into one that
    caches results in the filesystem and can coordinate multiple invocations
    of the same function + arguments via the filesystem
    '''
    def wrapper(*args, **kwargs):
        return CacheEntry(CACHE_ROOT, func, args, kwargs).run()
    return wrapper


class NotCachedError(Exception):
    pass


class CacheEntry(object):

    def __init__(self, root, func, args, kwgs):
        self.root = root
        self.func = func
        self.args = args
        self.kwgs = kwgs

    def run(self):
        '''
        if locked then wait for running process to finish.  if
        it finished succesfully we will return the cached result,
        if it failed we retry the function invocation.
        '''
        try:
            return self.load_cached_results()
        except NotCachedError:
            if self.locked:
                print("waiting for lock to release:", self.lock_path)
                while self.locked:
                    time.sleep(0.1)
                return self.run()
            with self.lock:
                with self.status as status:
                    print("caching under:", self.path)
                    status["started"] = time.time()
                    results = self.func(*self.args, **self.kwgs)
                    self.save_to_cache(results)
                    status["ended"] = time.time()
                    return results

    @defer
    def name(self):
        return self.func.__name__

    @defer
    def jail(self):
        return pyfs.jail(self.path)

    @defer
    def path(self):
        return pyfs.join(self.root, self.name, hashargs(self.args, self.kwgs))

    data_path = "cached"
    stat_path = "status"

    @property
    def locked(self):
        return self.lock.locked

    @defer
    def lock(self):
        return self.jail.lock()

    def load_cached_results(self):
        try:
            with self.jail.open(self.data_path, "rb") as fd:
                return pickle.load(fd)
        except pyfs.FileNotFoundError:
            self.clear_cache()
            raise NotCachedError

    def clear_cache(self):
        self.jail.rmtree()
        self.lock.release()

    def save_to_cache(self, data):
        try:
            with self.jail.open(self.data_path, "wb") as fd:
                return pickle.dump(data, fd)
        except OSError:
            raise NotCachedError

    @defer
    def status(self):
        return pyfs.store(self.stat_path)

    def cached(self):
        return self.jail.exists(self.data_path)

    @defer
    def null(self):
        return open('/dev/null', 'wb')

    def __repr__(self):
        return "CacheFS('%s','%s')" % (self.name, self.path)
