#!/usr/bin/env python

import numpy as np

from imaging import filters

np.seterr(all='raise')

NAMES = ["mrc", "rec", "ali", "mrcs", "ctf", "epa"]

MRCModes = {
    0: np.dtype("u1"),
    1: np.dtype("i2"),
    2: np.dtype("f4"),
    3: np.dtype("2i4"),
    4: np.dtype("c32"),
    6: np.dtype("u2"),
}

MRCHeader = np.dtype([
    ("dims", "3u4"),
    ("mode", "i4"),
    ("start", "3i4"),
    ("intervals", "3i4"),
    ("lengths", "3f4"),
    ("angles", "3f4"),
    ("mapping", "3i4"),
    ("min", "f4"),
    ("max", "f4"),
    ("mean", "f4"),
    ("spacegroup", "f4"),
    ("extended_header", "i4"),
    ("extra", "25i4"),
    ("origin", "3f4"),
    ("type", "i4"),
    ("stamp", "i4"),
    ("stdev", "f4"),
    ("nlabels", "i4"),
    ("labels", "10a80")
])


def load(path, supress=False, norm=False):
    endian = '<'
    with open(path, "rb") as fd:
        header = np.fromfile(fd, dtype=MRCHeader, count=1)[0]
        header = header.newbyteorder(endian)
        # memory map using copy on write mode
        # with offset starting after the header
        # and reverse dimensions from header
        if header["mode"] not in MRCModes:
            endian = '>'
            header = header.newbyteorder(endian)
        data = np.memmap(fd, mode="c",
                           offset=MRCHeader.itemsize + header['extended_header'],
                            dtype=MRCModes[header["mode"]],
                            shape=tuple(header["dims"][::-1]))
        data = data.newbyteorder(endian)
        if supress:
            data = filters.supress(data, *supress)
        if norm:
            data = filters.norm(data, *norm)
        return data


def load_header(path):
    with open(path, "rb") as fd:
        header = np.fromfile(fd, dtype=MRCHeader, count=1)[0]
        if header["mode"] not in MRCModes:
            header = header.byteswap()
        if header["mode"] not in MRCModes:
            raise ValueError('unknown MRC type')
        return header


def header_from_array(array):
    header = np.zeros([1], dtype=MRCHeader)
    dtype = np.dtype('%s%d' % (array.dtype.kind, array.dtype.itemsize))
    header[0]['mode'] = {MRCModes[k]: k for k in MRCModes}[dtype]
    shape = list(array.shape)
    if array.ndim == 2:
        shape = [1] + shape
    elif array.ndim == 4 and array.shape[1] == 1:
        shape = shape[0:1] + shape[2:]
    header[0]['dims'][:] = shape[::-1]
    header[0]['intervals'][:] = header[0]['dims']
    header[0]['lengths'][:] = header[0]['dims']
    header[0]['angles'] = 90.0
    return header


def header_from_dims(dims):
    header = np.zeros([1], dtype=MRCHeader)
    dtype = np.dtype('f4')
    header[0]['mode'] = {MRCModes[k]: k for k in MRCModes}[dtype]
    shape = list(dims)
    header[0]['dims'][:] = shape[::-1]
    header[0]['intervals'][:] = header[0]['dims']
    header[0]['lengths'][:] = header[0]['dims']
    header[0]['angles'] = 90.0
    header[0]['mapping'] = [ 2, 1 ,0 ]
    return header


def save(image, path):
    image = np.array(image)
    with open(path, 'wb') as dst:
        header = header_from_array(image)
        header.tofile(dst)
        image.tofile(dst)



