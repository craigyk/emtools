#!/usr/bin/env cky-python

import particles
import pystar2
import numpy as np
from scipy.spatial import cKDTree as KDTree
from collections import defaultdict as ddict

def load(path):
  return list(pystar2.load(path)[''].values())[0]


def save(particles, path):
  keys = particles.dtype.names
  pystar2.save( { '': { keys: particles } }, path)


def positions(particles):
  shifted_x = particles['rlnCoordinateX'].astype('f4') - particles['rlnOriginX'].astype('f4')
  shifted_y = particles['rlnCoordinateY'].astype('f4') - particles['rlnOriginY'].astype('f4')
  return np.array([shifted_x, shifted_y]).T


def dedup(particles, radius):
  grouped = ddict(list)
  for particle in particles:
    grouped[particle['rlnMicrographName']] += [tuple(particle)]
  cleaned = []
  for image in grouped:
    group = np.array(grouped[image], dtype=particles.dtype)
    tree  = KDTree(positions(group))
    pairs = tree.query_pairs(radius)
    keep  = connected_components(len(group), pairs)
    #if len(pairs) > 0:
      #print('image:', image, 'has', len(pairs), 'duplicates')
      #print(pairs)
      #print(keep)
      #print('-----')
    for idx in keep:
      cleaned += [tuple(group[idx])]
  return np.array(cleaned, dtype=particles.dtype)


def connected_components(size, pairs):
  comps = list(range(size))
  def search(idx):
    while idx != comps[idx]:
      idx = comps[idx]
    return idx  
  for p1, p2 in pairs:
    parent = search(p1)
    comps[p1] = parent
    comps[p2] = parent
  return set(comps)


if __name__ == '__main__':
  import sys
  radius = float(sys.argv[1])
  src = sys.argv[2]
  dst = sys.argv[3]
  print('loading...')
  ps = particles.load(src)
  print('deduping...')
  fs = particles.dedup(ps, radius)
  print('removed: %d duplicates'%(len(ps)-len(fs)))
  print('saving...')
  particles.save(fs, dst)



