from __future__ import absolute_import

from .formats import load, save, FORMATS
import imaging.fft as fft
import imaging.filters as filters
import imaging.drawing as drawing
import imaging.detection as detection


def rgba(r, g, b, a=1.0):
    return (b, g, r, a)

