
import cv2
import numpy as np


def fix_vec(vector, shift):
    scale = 2**shift
    return tuple(map(lambda x: int(round(x*scale)), vector[::-1]))


def circle(image, center, radius, thickness, color):
    shift = 4
    if thickness <= 1:
        # bug in opencv
        shift = 0
    scale = 2**shift
    center = fix_vec(center, shift)
    radius = int(round(radius*scale))
    thickness = int(thickness)
    cv2.circle(image, center, radius, color, thickness, cv2.LINE_AA, shift)
    return image


def ellipse(image, center, axes, angle, thickness, color):
    shift = 4
    center = fix_vec(center, shift)
    axes = fix_vec(axes, shift)
    cv2.ellipse(image, center, axes, angle, 0, 360, color, int(thickness), cv2.LINE_AA, shift)
    return image


def line(image, start, end, color, thickness):
    shift = 4
    start = fix_vec(start, shift)
    end = fix_vec(end, shift)
    cv2.line(image, start, end, color, int(thickness), cv2.LINE_AA, shift)
    return image
