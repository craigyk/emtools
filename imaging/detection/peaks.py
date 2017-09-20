

import numpy as np
from scipy import optimize

from numba import jit


def maxima(data, size):
    # wrapped the jit subfunction because if the new array
    # is created within the jit code it leaks memory
    output = np.zeros(data.shape, dtype=int)
    if data.ndim == 3:
        _local_maxima_3d(data, output, size)
    elif data.ndim == 2:
        _local_maxima_2d(data, output, size)
    for index in zip(*np.where(output > 0)):
        yield index, data[index]


@jit
def _local_maxima_3d(a, dst, size):
    for sec in range(0, a.shape[0]-size):
        for row in range(size, a.shape[1]-size):
            for col in range(size, a.shape[2]-size):
                dst[sec, row, col] = local_maximum_3d(a, sec, row, col, size)
    return dst


@jit
def _local_maxima_2d(a, dst, size):
    for row in range(size, a.shape[0]-size):
        for col in range(size, a.shape[1]-size):
            dst[row, col] = local_maximum_2d(a, row, col, size)
    return dst


@jit
def local_maximum_3d(a, sec, row, col, size):
    value = a[sec, row, col]
    for ss in range(sec, sec+size+1):
        for rs in range(row-size, row+size+1):
            for cs in range(col-size, col+size+1):
                if ss != sec or rs != row or cs != col:
                    if a[ss, rs, cs] >= value:
                        return 0
    return 1


@jit
def local_maximum_2d(a, row, col, size):
    value = a[row, col]
    for rs in range(row-size, row+size+1):
        for cs in range(col-size, col+size+1):
            if rs != row or cs != col:
                if a[rs, cs] >= value:
                    return 0
    return 1


def minima(data, size):
    for index, value in maxima(-data, size):
        yield index, -value


def neighbors_2d(row, col, size):
    return np.indices([size*2+1]*2).reshape([2, -1]).transpose() + [row, col] - size


def indices(data):
    return np.indices(data.shape).reshape([data.ndim, -1]).transpose()


def interpolator(data):
    points = indices(data)
    return scipy.interpolate.LinearNDInterpolator(points, data.flatten())


def neighbors(size):
    size = np.array(size)
    idxs = np.indices(size).reshape([size.size, -1]).T - ( size / 2 )
    return idxs


def gaussian_2d(amp, cx, cy, psi, sx, sy, x, y):
    tsin = np.sin(psi)
    tcos = np.cos(psi)
    a1 = (((x-cx)*tcos - (y-cy)*tsin)/sx)**2
    a2 = (((y-cy)*tcos + (x-cx)*tsin)/sy)**2
    return amp*np.exp(-(a1+a2))


def estimate_gaussian2d(data, row, col):
    amp = data[row, col]
    try:
        sigx = np.sqrt(np.abs(-1.0/(np.log(data[row  , col+1]/amp))))
        sigy = np.sqrt(np.abs(-1.0/(np.log(data[row+1, col]/amp))))
    except:
        sigx = 1.0
        sigy = 1.0
    psi = np.atan2(sigy, sigx)
    return amp, col, row, psi, sigx, sigy


def fit_gaussian2d(data, row, col):
    points = neighbors_2d(row, col, 1)

    def error(params):
        amp, cx, cy, psi, sx, sy = params
        total = []
        for row, col in points:
            if row < 0:
                row = 0
            if col < 0:
                col = 0
            if row >= data.shape[0]:
                row = data.shape[0]-1
            if col >= data.shape[1]:
                col = data.shape[1]-1
            total += [gaussian_2d(amp, cx, cy, psi, sx, sy, col, row) - data[row, col]]
        return total

    try:
        start = estimate_gaussian2d(data, row, col)
        solution, _ = optimize.leastsq(error, start)
        return solution
    except:
        return None


def fit2d(data, row, col):
    row, col = int(row), int(col)
    dxx = data[row,   col+1] + data[row,   col-1] - 2*data[row, col]
    dyy = data[row+1, col  ] + data[row-1, col]   - 2*data[row, col]
    dxy = data[row+1, col+1] - data[row+1, col-1] - data[row-1, col+1] + data[row-1, col-1]
    values, vectors = np.linalg.eig([[dxx, dxy],
                                     [dxy, dyy]])
    values = np.abs(values)
    psi = np.atan2(vectors[0, 0], vectors[0, 1])
    if values[0] > values[1]:
        return values[0], values[1], np.degrees(psi)
    return values[1], values[0], 180-np.degrees(psi)


@jit
def draw_gaussian(canvas, amp, cx, cy, psi, sx, sy):
    for row in range(canvas.shape[0]):
        for col in range(canvas.shape[1]):
            canvas[row,col] = gaussian_2d(amp,cx,cy,psi,sx,sy,col,row)
    return canvas

if __name__ == "__main__":
    import time
    from imaging import save, load
    a = draw_gaussian(np.zeros([512,512]), 1.0, 255.7, 256.4, np.radians(21.2), 20, 100)
    save(a, "temp_a.png")

    t0 = time.time()
    params = fit_gaussian2d(a,256,256)
    print( "took:", time.time() - t0)
    print( "amp:", params[0])
    print( "center:", params[2], params[1])
    print( "sigmas:", params[4], params[5])
    print( "psi:", np.degrees(params[3]) % 360)

    b = draw_gaussian(np.zeros([512,512]), *params)
    save(b, "temp_b.png")

    c = np.zeros([4000,4000])
    c[23,43] = 1.0
    c[443,3242] = 1.0
    print(list(maxima(c, 3)))

    c = np.zeros([128,128,128])
    c[123,121,102] = 1.0
    print(list(maxima(c, 3)))


