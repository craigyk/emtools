
from collections import OrderedDict as odict

from .frames import decode_frames, encode_frames
from .loops import decode_loops, encode_loops
from .values import decode_kv_pairs, encode_kv_pairs


def decode_blocks(lines):
    outer, blocks = split_blocks(lines)
    for key in blocks:
        blocks[key] = decode_block(blocks[key])
    return outer, blocks


def decode_block(lines):
    outer, frames = decode_frames(lines)
    outer, loops = decode_loops(outer)

    block = decode_kv_pairs(outer)

    for key in frames:
        block[key] = frames[key]

    for key in loops:
        block[key] = loops[key]

    return block


def split_blocks(lines):

    def parse_block_name(line):
        return line.split()[0][len('data_'):]

    def block_starts(line):
        return line.startswith('data_')

    blocks = odict()
    block = None
    outer = []

    for line in lines:
        if block_starts(line):
            name = parse_block_name(line)
            if name not in blocks:
                blocks[name] = []
            block = blocks[name]
        elif block is not None:
            block.append(line)
        else:
            outer.append(line)
    return outer, blocks


def encode_blocks(document):
    lines = []
    for name in document:
        lines.append('data_%s' % name)
        lines.append(encode_block(document[name]))
    return '\n'.join(lines)


def encode_block(block):
    lines = []
    lines.append(encode_kv_pairs(block))
    lines.append(encode_frames(block))
    lines.append(encode_loops(block))
    lines = [l for l in lines if len(l)]
    return '\n'.join(lines)
