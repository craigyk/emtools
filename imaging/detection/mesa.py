

import numpy as np

import pyfs

from imaging import fft, filters, load, save
from imaging.detection import keypoints, peaks


def mkring(dims, radius, width):
    inner_circle = mkcircle(dims, radius + 0.5)
    outer_circle = mkcircle(dims, radius * width + 0.5) - inner_circle
    inner_circle = inner_circle / np.sum(inner_circle)
    outer_circle = outer_circle / np.sum(outer_circle)
    templ = outer_circle - inner_circle
    return templ


def mkcircle(dims, radius):
    center = np.array(dims) / 2.0 - 0.5
    cimage = np.zeros(dims)
    indexs = np.indices(dims).transpose() - center
    radiis = np.sum(indexs**2, axis=-1)
    masked = np.sqrt(radiis) - radius
    circle = np.clip(masked, 0.0, 1.0)
    return 1.0 - circle


def mkoctave(image, start, stop, levels, ring):
    padby = int(stop*2)
    image = filters.pad(image, padby)
    radii = ring_sizes(start, stop, levels)
    octave = []
    for radius in radii:
        octave += [filters.unpad(mklevel(image, radius, ring), padby)]
    sizes = [radius*2 for radius in radii]
    return sizes, np.array(octave)


def mkpyramid(image, start, stop, levels, ring):
    return [mkoctave(image, start, stop, levels, ring)]


def mklevel(image, radius, ring):
    template = mkring(image.shape, radius, ring)
    convolved = fft.convolve(image, template)
    return filters.gaussian(convolved, 2.0)


def ring_sizes(start, stop, levels):
    sizes = [start]
    increment = 2**(1.0/levels)
    while sizes[-1] < stop:
        sizes += [sizes[-1]*increment]
    return sizes


def mesa(image, start=2.0, stop=None, levels=10, ring=1.05, psize=1, edge_threshold=2, threshold=1.0):
    stop = stop or min(image.shape) / 4
    print("mesa finding image particles:")
    print("   image dimensions:", image.shape)
    print("   from %f to %f" % (start, stop))
    print("   over %d levels" % (levels))
    print("   peak neighbor size: %d" % (psize))
    image = filters.norm(image, 5, 5, 0.0, 1.0)
    pyramid = mkpyramid(image, start, stop, levels, ring)
    features = find_features(pyramid, threshold, edge_threshold, psize)
    return features


def find_features(pyramid, threshold, edge_threshold, psize):
    features = []
    for o, (sizes, octave) in enumerate(pyramid):
        for (level, row, col), value in peaks.maxima(octave, psize):
            value *= 80
            if value > threshold:
                s1, s2, psi = peaks.fit2d(octave[level], row, col)
                if s2 != 0 and ((s1 / s2) < edge_threshold):
                    keypoint = keypoints.Keypoint(col, row, sizes[level], psi, value, o , 1)
                    features += [keypoint]
    return sorted(features, key=lambda x: x.response)


def binby(items, binner):
    bins = {}
    for item in items:
        bin = binner(item)
        if bin not in bins:
            bins[bin] = []
        bins[bin] += [item]
    return bins


def save(image, keys, root):
    image = filters.norm(image, 5, 5, 0, 255)
    key_sizes = binby(keys, lambda x: int(x.size))
    for size in key_sizes:
        drawn = keypoints.draw(image, key_sizes[size])
        save(drawn, pyfs.join(root, "%f.png"%size))


def save_pyramid(pyramid, root):
    for radii, octave in pyramid:
        save_octave(radii, octave, root)


def save_octave(radii, octave, root):
    levels = filters.norm(octave, 1, 1, 0, 255)
    for size, level in zip(radii, levels):
        save(level, pyfs.join(root, "%2.2f.png"%(size)), norm=False)


def test():
    path   = "cache/13apr24b_00002sq_v02_00004hl_v01_00002en.mrc"
    start  = 2
    stop   = 80
    levels = 10
    ring   = 1.2
    factor = 0.25
    psize  = 2
    emax   = 4.0
    vmin   = 0.01
    image = filters.zoom(load(path), factor)
    pyramid = mkpyramid(image, start, stop, levels, ring)
    features = find_peaks(pyramid, vmin, emax, psize)
    save_pyramid(pyramid, "out")
    save(keypoints.draw(image, features[-3000:]), "out.png")


if __name__ == "__main__":
    test()



