from __future__ import absolute_import

import pyfs
import weakref
import numpy as np
import pyfftw

import imaging.filters as filters


class wisdom(object):

    paths = [
        'pyfftw.wisdom'
    ]

    @classmethod
    def load(cls):
        try:
            import pickle
            with pyfs.aopen(cls.paths[0], 'rb') as src:
                wisdom = pickle.load(src)
            print(wisdom)
            pyfftw.import_wisdom(wisdom)
        except pyfs.errors.FileNotFoundError:
            pass

    @classmethod
    def save(cls):
        try:
            import pickle
            wisdom = pyfftw.export_wisdom()
            with pyfs.aopen(cls.paths[0], 'wb') as dst:
                pickle.dump(wisdom, dst)
        except IOError:
            pass

    @classmethod
    def reset(cls):
        pyfftw.forget_wisdom()
        cls.save()

    @classmethod
    def measure(cls, a):
        b = pyfftw.builders.rfftn(a, planner_effort='FFTW_MEASURE')()
        c = pyfftw.builders.irfftn(b, planner_effort='FFTW_MEASURE')()
        cls.save()
        return c

wisdom.load()


def wcache(func):
    '''
    this decorator is specifically to cache FFTs of images as long
    as the image still exists.  It uses reakrefs to the source images
    to store the I/FFTs.
    '''
    kcache = {}

    def wcached(arg):
        oid = id(arg)
        if oid not in kcache:
            def remove(r):
                del kcache[oid]
            ref = weakref.ref(arg, remove)
            kcache[oid] = (ref, func(arg))
        return kcache[oid][1]

    return wcached


@wcache
def rfft(data):
    datafft = pyfftw.builders.rfftn(data)()
    wisdom.save()
    return datafft


@wcache
def irfft(data):
    return pyfftw.builders.irfftn(data)()


def convolve(src, dst):
    src_fft = np.fft.rfftn(src)
    dst_fft = np.fft.rfftn(dst)
    return np.fft.fftshift(np.fft.irfftn(src_fft * dst_fft))


def correlate(src, dst):
    src_fft = np.fft.rfftn(padto(src, maxdims(src, dst)))
    dst_fft = np.fft.rfftn(padto(ndflip(dst), maxdims(src, dst)))
    return np.fft.fftshift(np.fft.irfftn(src_fft * np.conj(dst_fft)))


def ndflip(data):
    slices = [slice(None, None, -1)] * data.ndim
    return data[tuple(slices)]


def maxdims(src, dst):
    print(src.shape, dst.shape)
    return tuple(np.max([src.shape, dst.shape], axis=0))


def padto(data, dims):
    padding = []
    for dst, src in zip(dims, data.shape):
        left = dst - src
        half = left / 2
        padding += [(half, left-half)]
    return np.pad(data, tuple(padding), mode="edge")


def runpad(data, dims):
    padding = ( data.shape - dims ) / [2, 1]
    return filters.unpad(data, padding=tuple(padding))


def r_center(fft):
    return center(fft, axes=range(fft.ndim-1))


def r_uncenter(fft):
    return uncenter(fft, axes=range(fft.ndim-1))


def center(fft, axes=None):
    return np.fft.fftshift(fft, axes)


def uncenter(fft, axes=None):
    return np.fft.ifftshift(fft, axes)


def power(fft):
    return np.abs(fft)**2


def rmesh(shape):
    # a real fft is half-width on the last dimension, calculate center accordingly
    center = np.array(shape) / 2.0
    center[-1] = 0.0
    return np.indices(shape) - center.reshape([2]+[1]*len(shape))


def inbound(shape, shift):
    '''
    given a shift parameter and a set of image bounds, returns the array slices that are within range
    '''
    dst = []
    for ndim, offset in enumerate(shift):
        if offset < 0:
            dst += [slice(0, shape[ndim]+offset)]
        else:
            dst += [slice(offset, shape[ndim])]
    return dst


def shift(image, shift):

    imagefft = r_center(rfft(image))

    # generate the fft space coeffs
    # the equation for the phase shift is e^(-2j*pi*(x*(xshift/xdim)+y*(yshift/ydim)))
    idxs = rmesh(imagefft.shape)
    norm = np.array(shift, dtype='float64') / np.array(image.shape)
    norm = idxs * norm.reshape([2]+[1]*image.ndim)
    norm = np.sum(norm, axis=0)
    phases = np.exp(0.0-2j*np.pi*norm)

    # shift the image fft phases then take inverse transform
    rolled = irfft(r_uncenter(imagefft * phases))

    # because of the fft we now have an image that has been rolled
    # so we need to extract the valid image parts on a background
    shifted = np.zeros(image.shape) + np.mean(image)
    bounds = inbound(image.shape, np.floor(shift))
    shifted[bounds] = rolled[bounds]

    return shifted


