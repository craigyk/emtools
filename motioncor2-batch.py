#!/usr/bin/env cky-python

import os
import sys
import pyfs
import shutil
import tempfile
import multiprocessing as mp

import unpack

def get_arguments():

  import argparse

  parser = argparse.ArgumentParser(
    description='aligns byte-packed SerialEM images using motioncor2')

  parser.add_argument('-i', '--mrcs', nargs='+', required=True,
                        help='paths to SerialEM images')
  parser.add_argument('-d', '--defects', default=None,
                        help='Digital Micrograph defects file')
  parser.add_argument('-n', '--norm', default=None,
                        help='Digital Micrograph normalization file')
  parser.add_argument('-l', '--label', default='aligned',
                        help='label to attach to aligned images')
  parser.add_argument('-a', '--apix', required=True, type=float,
                        help='pixel size of images in Angstroms')
  parser.add_argument('-v', '--kv', default=None, type=float,
                        help='KV of microscope')
  parser.add_argument('-e', '--expf', default=None, type=float,
                        help='electrons per angstrom per frame if dose compensation is desired')
  parser.add_argument('-b', '--bfactor', default=100.0, type=float,
                        help='bfactor used for alignment')
  parser.add_argument('--gpus', default=1, type=int,
                        help='number of gpus to run on')
  parser.add_argument('--group', default=1, type=int,
                        help='number of frames to group to improve SNR')
  parser.add_argument('--patches', default=5, type=int,
                        help='number of patches for local refinement')
  parser.add_argument('-u', '--unpack', default=False, action='store_true',
                        help='unpack byte-packed frames')
  return parser.parse_args()




def motioncor2(src, dst, gain=None, expf=None, group=1, bfactor=200, kv=300, apix=1.0, patches=1, gpu=0):
  cmd  = ['motioncor2']
  if src.endswith('.mrc'):
    cmd += ['-InMrc', src]
  elif src.endswith('.tif'):
    cmd += ['-InTiff', src]
  else:
    raise ArgumentError('file: %s must be TIFF or MRC'%(src))
  cmd += ['-OutMrc', dst]
  cmd += ['-Bft', bfactor]
  cmd += ['-Patch', patches, patches]
  cmd += ['-Kv', kv]
  cmd += ['-PixSize', apix]
  cmd += ['-Group', group]
  if expf is not None:
    cmd += ['-FmDose', expf]
  cmd += ['-Gpu', gpu]
  if gain is not None:
    cmd += ['-Gain', gain]
  os.system(' '.join(map(str, cmd))) 


def pbunzip2(src, dst):
  if src.endswith('.bz2'):
    os.system('pbunzip2 %s -c > %s'%(src, dst))
    return dst
  elif src.endswith('.zst'):
    os.system('pzstd -d -o %s %s'%(dst, src))
    return dst
  else:
    return src


def process(path, aligned, args, gpu, should_unpack):
  
  aligned_dw = unpack.label(aligned, 'dw')
  
  print(path, '->', aligned)
  
  tmpdir = tempfile.mkdtemp()
  tmp_unzipped    = pyfs.join(tmpdir, 'decompressed.mrc')
  tmp_unpacked    = pyfs.join(tmpdir, 'unpacked.mrc')
  tmp_aligned     = pyfs.join(tmpdir, 'aligned.mrc')
  tmp_aligned_dw  = pyfs.join(tmpdir, 'aligned_DW.mrc')
  
  tmp_unzipped = pbunzip2(path, tmp_unzipped)
  
  if should_unpack: 
    unpack.unpack(tmp_unzipped, tmp_unpacked, args.defects, args.norm, mode='byte')
    motioncor2(tmp_unpacked, tmp_aligned, gain=None, bfactor=args.bfactor, group=args.group, expf=args.expf, kv=args.kv, apix=args.apix, patches=args.patches, gpu=gpu)
  else:
    motioncor2(tmp_unzipped, tmp_aligned, gain=args.norm, expf=args.expf, bfactor=args.bfactor, group=args.group, kv=args.kv, apix=args.apix, patches=args.patches, gpu=gpu)  

  shutil.copy(tmp_aligned, aligned)
  if pyfs.exists(tmp_aligned_dw):
    shutil.copy(tmp_aligned_dw, aligned_dw)
  shutil.rmtree(tmpdir)


def label(path, label):
  path = path.replace('.bz2','')
  path = unpack.label(path, label)
  return path


def main():
  
  args = get_arguments()
  pool = mp.Pool(args.gpus)
  
  gpu = 0
  for path in args.mrcs:
    dst = label(path, args.label)
    if pyfs.exists(dst):
      print(dst, 'exists, skipping', path)
      continue
    #print(path, '->', dst)
    #process(path, dst, args, gpu % args.gpus)
    pool.apply_async(process, args=(path, dst, args, gpu % args.gpus, args.unpack))
    gpu += 1
  pool.close()
  pool.join()


if __name__ == '__main__':
  main()



