
import math

import numpy as np

import pyfs

from imaging.detection import peaks
from imaging.detection import keypoints
from imaging import filters, save


def mk_sigmas(start, stop, bins=7):
    '''
    given a sigma range and a number of bins, return a list of sigma values
    '''
    np.exp(np.log(2) / levels)
    sigmas = []
    current_bin = 1
    sigma = 1.5


def dog_pyramid2(image, start=1.5, stop=None, levels=7):

    octaves = []

    image = filters.gaussian(image, start)

    stop = stop or min(image.shape) / 8
    sigma = start
    increment = 2**(1.0 / (levels))

    while min(*image.shape) >= 16:

        octave = len(octaves)
        gaussians = [(sigma, image)]
        for i in range(levels + 2):
            sigma *= increment
            lsigma, limage = gaussians[-1]
            dsigma = sigma_diff(lsigma, sigma)
            gaussians += [(sigma, filters.gaussian(limage, dsigma))]

        dogs = []
        for level in range(len(gaussians) - 1):
            s1, i1 = gaussians[level]
            s2, i2 = gaussians[level + 1]
            size = (2**octave) * dog_size(s1, s2)
            dogs += [(size, i2 - i1)]

        image = filters.zoom(gaussians[-3][1], 0.5)
        sigma = gaussians[-3][0] / 2.0

        octaves += [dogs]

    return octaves


def correct_pyramid(pyramid, size):
    return pyramid


def mkpyramid(image, start=1.5, stop=None, levels=7):

    sigmas = [start]
    gaussians = [filters.gaussian(image, start)]

    sigma = start
    stop = stop or min(image.shape) / 8
    increment = np.exp(np.log(2) / levels)

    while sigma < stop:
        sigma *= increment
        dsigma = sigma_diff(sigmas[-1], sigma)
        gaussians += [filters.gaussian(gaussians[-1], dsigma)]
        sigmas += [sigma]

    dogs = []
    sizes = []
    for level in range(len(gaussians) - 1):
        i1, i2 = gaussians[level:level + 2]
        s1, s2 = sigmas[level:level + 2]
        # the *2 is to convert from radius to diameter
        sizes += [dog_size(s1, s2) * 2]
        dogs += [i2 - i1]

    octave = np.array(dogs)

    return [(sizes, octave)]


def sigma_diff(s1, s2):
    return math.sqrt((s2**2) - (s1**2))


def dog_size(s1, s2):
    k = s2 / s1
    k2 = k**2
    k21 = k2 - 1
    return s1 / math.sqrt(k21 / (2 * k2 * math.log(k)))


def dog(image, start=1.0, stop=None, levels=3, psize=1, threshold=0.0, edge_threshold=3.0):

    image = image.astype("f8")

    threshold *= 5.0

    features = []
    pyramid = mkpyramid(image, start=start, stop=stop, levels=levels)
    for o, (sizes, octave) in enumerate(pyramid):
        for (level, row, col), value in peaks.maxima(octave, psize):
            if value > threshold:
                s1, s2, psi = peaks.fit2d(octave[level], row, col)
                if s2 != 0 and ((s1 / s2) < edge_threshold):
                    keypoint = keypoints.Keypoint(
                        col, row, sizes[level], psi, value, level, 1)
                    features += [keypoint]

    save(pyramid, features, "levels")

    return features


def binby(items, binner):
    bins = {}
    for item in items:
        bin = binner(item)
        if bin not in bins:
            bins[bin] = []
        bins[bin] += [item]
    return bins


def save(pyramid, keys, root):
    sizes, octave = pyramid[0]
    octave = filters.norm(octave, 5, 5, 0, 255)
    key_levels = binby(keys, lambda x: int(x.octave))
    for l, level in enumerate(octave):
        drawn = level
        if l in key_levels:
            drawn = keypoints.draw(level, key_levels[l])
        save(drawn, pyfs.join(root, "%f.png" % sizes[l]), norm=False)


if __name__ == "__main__":
    import sys
    import imaging
    a = imaging.load(
        "/Users/craigyk/Desktop/classifier/cache/13apr24b_00002sq_v02_00002hl_v01_00003en.mrc")
    a = imaging.filters.zoom(a, 0.25)
    features = detect(a, start=1.5, levels=5)
    save(draw(a, features), sys.argv[1])
