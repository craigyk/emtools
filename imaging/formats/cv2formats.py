
import pyfs
import cv2

from imaging import filters

NAMES = ["png", "jpg", "tiff"]


def load(path):
    return cv2.imread(path)


def save(image, path, norm=(0.01, 0.01, 0, 255)):
    if norm:
        image = filters.norm(image, *norm)
    with pyfs.shadow(path):
        cv2.imwrite(path, image.astype('uint8'))
    return path


