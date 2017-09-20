from __future__ import absolute_import

import re

from .errors import *
from .cmds import ls, join, exists, split


def join_roots(roots, component):
    roots = [join(x, component) for x in roots]
    roots = [x for x in roots if exists(x)]
    return roots


def expand_roots(roots, component):
    regex = compile_regex(component)
    nroots = []
    for root in roots:
        try:
            names = ls(root)
        except NotADirectoryError:
            continue
        for fname in names:
            if regex.match(fname):
                nroots += [join(root, fname)]
    return nroots


def compile_regex(component):
    component = component.replace(".", "\.")
    component = component.replace("*", ".*")
    component = "^" + component + "$"
    return re.compile(component)


def is_fixed(path):
    chars = ["*", "[", "]", "\\"]
    return not any([x in path for x in chars])


def glob(pattern):
    roots = [""]
    for component in split(pattern):
        if is_fixed(component):
            roots = join_roots(roots, component)
        else:
            roots = expand_roots(roots, component)
    compiled = compile_regex(pattern)
    for root in roots:
        match = compiled.match(root)
        if match:
            groups = match.groups()
            if len(groups) > 0:
                yield (root,) + groups
            else:
                yield root
