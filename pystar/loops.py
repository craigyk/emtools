
import numpy as np
from collections import OrderedDict as odict

from .values import tokenize, decode_key, decode_value


def decode_loops(lines):
    outer, _loops = split_loops(lines)
    loops = odict()
    for fields, values in _loops:
        loops.update(decode_loop(fields, values))
    return outer, loops


def split_loops(lines):
    '''
    loops are stupid, they end with the end of a frame, data block, or implicitly
    if a field name is reached _after_ a string of values.  This makes parsing them
    hard.  at least we only have to worry about the loop -> fields -> values
    pattern, since this function is only called with `lines` from the same block or frame
    '''
    loops = []
    outer = []
    loop = None
    for line in lines:

        if line.startswith('loop_'):
            # a loop is a pair of lists.  one for keys, and one for values
            loop = [[], None]
            loops.append(loop)
        elif loop is None:
            # what we do if we are not in a loop
            outer.append(line)
        elif loop[1] is not None:
            # we are expecting values now
            if line[0] == '_':
                # we got a field, exit loop
                loop = None
                outer.append(line)
            else:
                loop[1].append(line)
        elif line[0] != '_':
            # we've reached a value
            loop[1] = []
            loop[1].append(line)
        else:
            loop[0].append(line)
    return outer, loops


def decode_loop(fields, values):

    def split_rows(values, fields):
        vlen = len(values)
        flen = len(fields)
        rows = [ tuple(values[idx:idx+flen]) for idx in range(0, vlen, flen) ]
        return rows

    fields = [decode_key(token) for token in tokenize(fields)]
    values = [decode_value(token) for token in tokenize(values)]

    cols = np.dtype([(field, 'O') for field in fields])
    rows = split_rows(values, fields)

    return { tuple(fields): np.array(rows, dtype=cols) }


def encode_loops(block):

    def block_loops(block):
        return [(k, block[k]) for k in block if isinstance(k, tuple)]

    lines = []
    for fields, loop in block_loops(block):
        lines.append(encode_loop(fields, loop))
    return '\n'.join(lines)


def encode_loop(fields, loop):
    lines = ['loop_']
    for col in fields:
        lines.append('_%s' % col)
    for row in loop:
        lines.append(" ".join([str(v) for v in row]))
    lines = [l for l in lines if len(l)]
    return '\n'.join(lines)
