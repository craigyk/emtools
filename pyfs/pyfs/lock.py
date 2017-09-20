

# This exists because OS shared and exclusive locks
# are not always consistent, especially on shared filesystems.
# This setup works by first having the thread create a unique file lock
# under the same path as the target path.  This file is then hardlinked
# to a deteministic lock path.  The linking operation should be atomic
# in which case only a single process will succeed in linking.
#
# Special care is also taken to clean up any and all files created during
# this process, even if the process is interrupted or aborted.
# Note: locks can be still be left if computers are shut down unexpectedly
#       or there are network problems

from __future__ import absolute_import

import os
import uuid
import time
import signal

from oneup import defer
from .cmds import rm, exists, create, ln, samefile
from .errors.errors import *


def lock(path, poll=0.1, timeout=None, block=True):
    return FSLock(path, poll=poll, timeout=timeout, block=block)


def locked(path):
    return FSLock(path).locked

# This tracking is to make sure that all locks get cleared
# if python gets killed, even in a subproceses or thread
ALL_LOCKS = {}
for event in [signal.SIGINT, signal.SIGTERM]:
    def handler(s, t):
        for lock in ALL_LOCKS.values():
            lock.release()
        if hasattr(old_handler, "__call__"):
            old_handler(s, t)
        raise RuntimeError("python process %d was interrupted with signal: %s" % (os.getpid(), s))
    old_handler = signal.signal(event, handler)


def track_lock(lock):
    ALL_LOCKS[lock.id] = lock
    return lock


def untrack_lock(lock):
    try:
        del ALL_LOCKS[lock.id]
    except KeyError:
        pass
    return lock
# --------------------------------------------------


class FSLock(object):

    def __init__(self, path, poll=0.1, timeout=None, block=True):
        self.path = path
        self.poll = poll
        self.block = block
        self.timeout = timeout
        self.started = None

    @defer
    def id(self):
        return uuid.uuid1()

    @property
    def locked(self):
        print("lock path: %s is locked: %o" % (self.mlockpath, exists(self.mlockpath)))
        return exists(self.mlockpath)

    @defer
    def mlockpath(self):
        # main lock path
        return "%s.lock" % (self.path)

    @defer
    def ulockpath(self):
        # this lock's unique lock path
        return '%s.%s.lock' % (self.path, self.id)

    def acquire(self):
        try:
            create(self.ulockpath)
            ln(self.ulockpath, self.mlockpath)
        except FileExistsError:
            pass
        if not self.owns_lock:
            self.release()
            return False
        return True

    @property
    def owns_lock(self):
        try:
            return samefile(self.mlockpath, self.ulockpath)
        except FileNotFoundError:
            return False

    def release(self, force=False):
        if force or self.owns_lock:
            rm(self.mlockpath)
        rm(self.ulockpath)
        untrack_lock(self)

    def __enter__(self):
        self.started = time.time()
        while True:
            if self.acquire():
                track_lock(self)
                return self
            elif not self.block:
                raise PathAlreadyLockedError(-1, "path is already locked", self.path)
            elif self.timeout and self.wait_time > self.timeout:
                raise TimeoutError(-1, "timedout waiting for lock", self.path)
            else:
                time.sleep(self.poll)
                if self.wait_time > 10:
                    print("[warning] file has been locked for > %2.2f secs" % (self.wait_time))

    @property
    def wait_time(self):
        if not self.started:
            return 0
        return time.time() - self.started

    def __exit__(self, et, ev, st):
        self.started = None
        self.release()
