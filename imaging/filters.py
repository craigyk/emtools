
import math

import cv2
import numpy as np
import scipy.ndimage as nd


def dtype_min(dtype):
    try:
        return np.iinfo(dtype).min
    except ValueError:
        return -1.0


def dtype_max(dtype):
    try:
        return np.iinfo(dtype).max
    except ValueError:
        return 1.0


def supress(image, threshold=10, neighbors=9):

    edges = np.abs(nd.filters.laplace(image))
    indices = np.where(edges > (threshold*np.std(edges)))
    indices = np.vstack(indices).transpose()

    masked = {}
    for x, y in indices:
        masked[(x, y)] = True

    supressed = np.copy(image)
    for x, y in indices:
        neighborhood = []
        radius = 1
        while len(neighborhood) < neighbors:
            for _x in (x-radius, x+radius):
                for _y in (y-radius, y+radius):
                    if (_x, _y) in masked:
                        continue
                    if _x < 0 or _x >= image.shape[0]:
                        continue
                    if _y < 0 or _y >= image.shape[1]:
                        continue
                    neighborhood += [supressed[_x, _y]]
            radius += 1
        supressed[x, y] = np.median(neighborhood)

    return supressed


def norm(image, minb=0.01, maxb=0.01, nminv=None, nmaxv=None):
    image = np.array(image)
    if nminv is None:
        nminv = dtype_min(image.dtype)
    if nmaxv is None:
        nmaxv = dtype_max(image.dtype)
    if minb < 1.0:
        minb = max(int(minb * image.size), 1)
    if maxb < 1.0:
        maxb = max(int(maxb * image.size), 1)
    maxb = image.size - np.clip(maxb, 1, image.size-1)
    minb = np.clip(minb, 1, image.size-1)
    sorted_values = np.sort(image, axis=None)
    try:
        ominv = np.mean(sorted_values[:minb])
        omaxv = np.mean(sorted_values[maxb:])
    except:
        print("couldn't get means of:", sorted_values[:minb], sorted_values[maxb:])
        print( "    :", minb, maxb, image)
        ominv = np.min(image.flat)
        omaxv = np.max(image.flat)
    try:
        image = (image-ominv)/(omaxv-ominv)
    except FloatingPointError:
        pass
    image = (image*(nmaxv-nminv))+nminv
    image = np.clip(image, nminv, nmaxv)
    return image


def invert(data):
    minv = np.min(data)
    maxv = np.max(data)
    return scale(-data, minv, maxv)


def scale(image, nminv, nmaxv):
    minv = np.min(image)
    maxv = np.max(image)
    return ((image-minv)/(maxv-minv))*(nmaxv-nminv) + nminv


def match_dims(ndim, x):
    try:
        if len(x) == ndim:
            return np.array(x)
    except TypeError:
        return match_dims(ndim, [x] * ndim)
    raise TypeError("inappropriate number of dimensions")


def pad(data, padding, mode="edge"):
    if mode == 'mean':
        return np.pad(data, padding, mode='constant', constant_vals=[np.mean(data)])
    return np.pad(data, padding, mode=mode)


def unpad(data, padding):
    slices = []
    if isinstance(padding, int):
        padding = [padding] * data.ndim
    for dim, pad in enumerate(padding):
        if isinstance(pad, int):
            pad = (pad, pad)
        slices += [slice(pad[0], data.shape[dim]-pad[1])]
    return data[slices]


def gaussian(image, sigma=0.5):
    return nd.filters.gaussian_filter(image, sigma, mode="nearest")


def laplace(image, sigma=0.5):
    return nd.filters.gaussian_laplace(image, sigma)


def window(image, center, size, mode="random"):

    size = match_dims(image.ndim, size)
    center = match_dims(image.ndim, center)
    if np.any(size <= 0):
        raise ValueError("size of window must be positive")

    if mode == "random":
        return window_random(image, center, size)
    elif mode == "clipped":
        return window_clipped(image, center, size)
    else:
        return window_padded(image, center, size, mode)


def window2(image, center, dims, mode='wrap'):
    matrix = np.array([[1, 0], [0, 1]])
    shift = np.array(center) - ( np.array(dims) / 2.0 )
    return nd.affine_transform(image, matrix, offset=shift, output_shape=dims, mode=mode)


def window_random(image, center, size):
    indexes = []
    padding = []
    for dim in range(image.ndim):
        loff = int(center[dim] - (size[dim]/2))
        roff = int(center[dim] + (size[dim]/2))
        lpad = int(max(0, -loff))
        rpad = int(min(size[dim], size[dim]-(roff-image.shape[dim])))
        lidx = int(max(0, loff))
        ridx = int(min(image.shape[dim], roff))
        indexes += [slice(lidx, ridx)]
        padding += [slice(lpad, rpad)]
    pulled = image[indexes]
    window = np.random.choice(pulled.flat, size)
    window[padding] = pulled
    return window


def window_padded(image, center, size, mode):
    padding = []
    centers = []
    for dim in range(image.ndim):
        loff = int(center[dim] - (size[dim]/2))
        roff = int(center[dim] + (size[dim]/2))
        lidx = int(max(0, loff))
        ridx = int(min(image.shape[dim], roff))
        lpad = int(lidx-loff)
        rpad = int(roff-ridx)
        centers += [slice(lidx, ridx)]
        padding += [(lpad, rpad)]
    return np.pad(image[centers], padding, mode=mode)


def window_clipped(image, center, size):
    centers = []
    for dim in range(image.ndim):
        loff = int(center[dim] - (size[dim]/2))
        roff = int(center[dim] + (size[dim]/2))
        lidx = int(max(0, loff))
        ridx = int(min(image.shape[dim], roff))
        centers += [slice(lidx, ridx)]
    return image[centers]


def resize(image, nrows, ncols):
    nrows = int(nrows)
    ncols = int(ncols)
    interpolation = modes.area
    if image.shape < (nrows, ncols):
        interpolation = modes.lanczos
    return cv2.resize(image, (nrows, ncols), interpolation=interpolation)


def zoom(image, factor):
    rows = int(np.round(float(image.shape[-2]) * factor))
    cols = int(np.round(float(image.shape[-1]) * factor))
    return resize(image, rows, cols)


def save(path, image):
    cv2.imwrite(path, image)


def asRGB(image):
    if image.ndim < 3:
        image = np.repeat(np.expand_dims(image, -1), 3, -1)
    else:
        image = np.copy(image)
    return scale(image, 0, 255).astype("u1")


class modes(object):

    wrap = cv2.BORDER_WRAP
    reflect = cv2.BORDER_REFLECT
    isolated = cv2.BORDER_ISOLATED
    replicate = cv2.BORDER_REPLICATE
    transparent = cv2.BORDER_TRANSPARENT
    border = cv2.BORDER_CONSTANT

    area = cv2.INTER_AREA
    cubic = cv2.INTER_CUBIC
    linear = cv2.INTER_LINEAR
    lanczos = cv2.INTER_LANCZOS4


def shift(image, x, y, border=modes.wrap):
    a = np.array([[0, 0],
                  [0, 1],
                  [1, 0]])
    t = get_affine_transform(a, a + [x, y])
    return affine_transform(image, t, border=border, inter=modes.cubic)


def get_affine_transform(a,b):
    return cv2.getAffineTransform(a.astype("f4"),b.astype("f4"))

def affine_transform(image,transform,border=modes.wrap,inter=modes.area):
    bconst = None
    if isinstance(border, (int,float)):
        bconst = float(border)
        border = modes.border
    return cv2.warpAffine(image,transform,None,flags=inter,borderMode=border,borderValue=bconst)

POINTS = np.array([[1.0,0.0],
                   [0.0,1.0],
                   [0.0,0.0]])
def shift_rotate(image,rotate,shift_x,shift_y,border=modes.wrap):
    cx = image.shape[1] / 2.0
    cy = image.shape[0] / 2.0
    center = np.array([cx,cy])
    shift  = np.array([shift_x,shift_y])
    sinr   = math.sin(math.radians(rotate))
    cosr   = math.cos(math.radians(rotate))
    src = POINTS + center
    dst = np.dot(POINTS-shift,[[cosr,sinr],[-sinr,cosr]]) + center
    transform = get_affine_transform(src,dst)
    return affine_transform(image,transform,border=border,inter=modes.cubic)


def random(dims, distribution="uniform", **kwargs):
    try:
        return getattr(np.random, distribution)(size=dims, **kwargs)
    except KeyError:
        raise NotImplemented("random distribution: %s is not implemented" % distribution)


def correlate(image, kernel):
    return nd.filters.correlate(image, kernel)
