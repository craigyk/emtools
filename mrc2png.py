#!/usr/bin/env cky-python

import sys
import glob
import pyfs
import time
import imaging
import numpy as np


def get_arguments():

  import argparse

  parser = argparse.ArgumentParser(
    description='watches a directory for files matching a pattern and coverts to PNG with given scaling')

  parser.add_argument('-i', '--pattern', required=True, nargs='+',  help='images to convert')
  parser.add_argument('-s', '--scale', type=float, default=1.0, help='scaling for saving image')
  parser.add_argument('-f', '--force', action='store_true', help='force overwriting of images')
  return parser.parse_args()


def topng(mrc, scale, force):
  png = pyfs.rext(mrc, 'png')
  png = mrc + '.png'
  if not force and pyfs.exists(png):
    return
  print(mrc, '->', png)
  image = imaging.load(mrc)[0]
  image = imaging.filters.zoom(image, scale)
  imaging.save(image, png)


if __name__ == '__main__':
  args = get_arguments()
  #mrcs = list(glob.glob(args.pattern))
  for mrc in args.pattern:
    topng(mrc, args.scale, args.force)




