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

def dist_filter(particles, dims, dist):
  dims = np.array(dims)
  xys = positions(particles)
  particles = particles[np.where(xys[:,0]>(dims[0]+dist))]
  xys = positions(particles)
  particles = particles[np.where(xys[:,1]>(dims[1]+dist))]
  xys = positions(particles)
  particles = particles[np.where(xys[:,0]<(dims[2]-dist))]
  xys = positions(particles)
  particles = particles[np.where(xys[:,1]<(dims[3]-dist))]
  return particles


def remove_on_edges(particles, padding):
  particles = dist_filter(particles, [0, 0] + padding[1:3], padding[0])
  return particles


if __name__ == '__main__':
  
  import argparse
  
  parser = argparse.ArgumentParser(
        description='clean up a STAR file of duplicate and/or border particles')

  parser.add_argument('-d', '--dedup', type=float, default=False,
                        help='radius (pixels) for determining wether particles are duplicates')
  parser.add_argument('-e', '--edge', default=False, type=int, nargs=3,
                        help='remove particles near edges, supply the edge radius and image dimensions (pixels) ie. 50 3760 3878')
  parser.add_argument('-i', '--src', required=True,
                        help='input STAR file')
  parser.add_argument('-o', '--dst', required=True,
                        help='outpout STAR file')


  args = parser.parse_args()


  print('loading...')
  ps = load(args.src)
  print('loaded: %d particles'%(len(ps)))
  if args.dedup is not False:
    print('deduping...')
    fs = dedup(ps, args.dedup)
    print('removed: %d duplicates'%(len(ps)-len(fs)))
    ps = fs
  if args.edge is not False:
    fs = remove_on_edges(ps, args.edge)
    print('removed: %d edge particles'%(len(ps)-len(fs)))
    ps = fs
  print('saving...')
  save(ps, args.dst)



