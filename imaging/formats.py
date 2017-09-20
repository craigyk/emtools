
import pyfs

# load formatting plugins
FORMATS = pyfs.loader(pyfs.join(pyfs.dpath(__file__), "formats"))


def save(data, path, format=None, **kwargs):
    if not format:
        format = pyfs.gext(path, last=True)
    return FORMATS[format].save(data, path, **kwargs)


def load(path, format=None, **kwargs):
    if not format:
        format = pyfs.gext(path, last=True)
    if format not in FORMATS:
        raise ValueError('file {file!s} is of unknown format'.format(file=path))
    return FORMATS[format].load(path, **kwargs)


def load_stats(path, **kwargs):
    path = pyfs.rext(path, "yaml")
    return load(path)


def save_stats(stats, path, **kwargs):
    path = pyfs.rext(path, "yaml")
    save(stats, path)
