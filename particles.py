#!/usr/bin/env cky-python

import particles
import pystar
import numpy as np
from scipy.spatial import cKDTree as KDTree
from collections import defaultdict as ddict

def load(path):
  return list(pystar.load(path)[''].values())[0]


def save(particles, path):
  keys = particles.dtype.names
  pystar.save( { '': { keys: particles } }, path)


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

def dist_filter(particles, origin, dist):
  origin = np.array(origin)
  xys = positions(particles)
  particles = particles[np.where(xys[:,1]<(origin[1]-dist))]
  xys = positions(particles)
  particles = particles[np.where(xys[:,0]<(origin[0]-dist))]
  xys = positions(particles)
  particles = particles[np.where(xys[:,1]>dist)]
  xys = positions(particles)
  particles = particles[np.where(xys[:,0]>dist)]
  return particles
  #d = np.sqrt(np.sum((c-origin)**2,axis=1))
  return particles[np.where(d > dist)]


def remove_on_edges(particles, padding):
  #particles = dist_filter(particles, [0, 0], padding)
  particles = dist_filter(particles, [3838, 3710], padding)
  return particles


if __name__ == '__main__':
  import sys
  radius = float(sys.argv[1])
  padding = float(sys.argv[2])
  src = sys.argv[3]
  dst = sys.argv[4]
  print('loading...')
  ps = load(src)
  print('deduping...')
  fs = dedup(ps, radius)
  print('removed: %d duplicates'%(len(ps)-len(fs)))
  rs = remove_on_edges(fs, padding)
  print('removed: %d edge particles'%(len(fs)-len(rs)))
  print('saving...')
  save(rs, dst)



