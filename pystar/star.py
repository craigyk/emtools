
from .blocks import encode_blocks, decode_blocks
from .strings import normalize_strings


class ParseError(Exception):
    pass


def preparse(string):
    lines = string.splitlines()
    lines = normalize_strings(lines)
    return lines


def decode_document(lines):
    lines = preparse(lines)
    extra, blocks = decode_blocks(lines)
    return blocks


def load(path):
    with open(path, 'r', encoding='ASCII') as src:
        document = decode_document(src.read())
        return document


def encode_document(document):
    return encode_blocks(document)


def save(document, path):
    with open(path, 'w') as dst:
        dst.write(encode_blocks(document))


if __name__ == '__main__':

    def equals(doc1, doc2):
        import numpy
        for key in doc1:
            if key not in doc2:
                return False
            elif type(doc1[key]) != type(doc2[key]):
                return False
            elif isinstance(doc1[key], (str, float)):
                if doc1[key] != doc2[key]:
                    return False
            elif isinstance(doc1[key], numpy.ndarray):
                if not all(doc1[key] == doc2[key]):
                    return False
            else:
                if not equals(doc1[key], doc2[key]):
                    return False
        return True

    model1 = load('/Users/yoshiokc/Desktop/frame-processing/dog.star')
    save(model1, '/Users/yoshiokc/Desktop/frame-processing/test.star')
    model2 = load('/Users/yoshiokc/Desktop/frame-processing/test.star')
    print(equals(model1, model2))
