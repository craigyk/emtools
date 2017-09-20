

from collections import OrderedDict as odict


def decode_value(token):
    try:
        return int(token)
    except ValueError:
        try:
            return float(token)
        except ValueError:
            return token


def decode_key(token):
    return token[1:]


def tokenize(lines):
    tokens = []
    for line in lines:
        if line[0] == ';':
            tokens.append(line[1:])
        else:
            tokens.extend(line.split())
    return tokens


def decode_kv_pairs(lines):
    tokens = tokenize(lines)
    assert(len(tokens) % 2 == 0)
    values = odict()
    for idx in range(0, len(tokens), 2):
        key = decode_key(tokens[idx])
        val = decode_value(tokens[idx+1])
        values[key] = val
    return values


def encode_kv_pairs(block):

    def block_kv_pairs(block):
        return [(k, block[k]) for k in block if isinstance(block[k], (float, str))]

    lines = []
    for key, value in block_kv_pairs(block):
        lines.append('_%s %s' % (key, encode_value(value)))
    lines = [l for l in lines if len(l)]
    return '\n'.join(lines)


def encode_value(value):
    if isinstance(value, str):
        if len(value.split()) > 1:
            value = '\n;\n' + value + '\n;'
    return str(value)
