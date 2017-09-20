
from collections import OrderedDict as odict
from .values import decode_kv_pairs, encode_kv_pairs
from .loops import decode_loops, encode_loops


def split_frames(lines):
    '''
    splits a list of lines into lines that are not part of a frame,
    and a list of lines, where each list is part of the same frame.
    frames start with a `save_` and end with a `stop_`.  They also
    end with the next data block, but this function is only called
    with lines from a single data block.
    '''

    def parse_frame_name(line):
        return line.split()[0][len('save_'):]

    def frame_starts(line):
        return line.startswith('save_')

    def frame_stops(line):
        return line.startswith('stop_')

    outer = []
    frame = None
    frames = odict()
    for line in lines:
        if frame_stops(line):
            frame = None
        elif frame_starts(line):
            name = parse_frame_name(line, 'save_')
            if name not in frames:
                frames[name] = []
            frame = frames[name]
        elif frame is None:
            outer.append(line)
        else:
            frame.append(line)
    return outer, frames


def decode_frames(lines):

    outer, frames = split_frames(lines)

    for key in frames:
        frames[key] = decode_frame(frames[key])

    return outer, frames


def decode_frame(lines):

    outer, loops = decode_loops(lines)
    frame = decode_kv_pairs(outer)

    for key in loops:
        frame[key] = loops[key]

    return frame


def encode_frames(block):

    def block_frames(block):
        return [(k, block[k]) for k in block if isinstance(block[k], dict)]

    lines = []
    for name, frame in block_frames(block):
        lines.append('save_%s' % name)
        lines.append(encode_frame(frame))
        lines.append('stop_')
    return '\n'.join(lines)


def encode_frame(frame):
    lines = []
    lines.append(encode_kv_pairs(frame))
    lines.append(encode_loops(frame))
    return '\n'.join(lines)
