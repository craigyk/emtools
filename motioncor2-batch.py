#!/usr/bin/env cky-python

import os
import sys
import pyfs
import shutil
import tempfile
import subprocess as sp
import multiprocessing as mp

import unpack

def call(args, stderr=None, stdout=None):
  return sp.run(map(str, args), stderr=stderr, stdout=stdout)


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
  parser.add_argument('-s', '--savestack', default=False, action='store_true',
                        help='save aligned stack')
  parser.add_argument('--binby', default=1, type=int,
                        help='binning by motioncor2')
  return parser.parse_args()




def motioncor2(src, dst, args, gpu=0):
  cmd  = ['motioncor2']
  if src.endswith('.mrc') or src.endswith('.mrcs'):
    cmd += ['-InMrc', src]
  elif src.endswith('.tif'):
    cmd += ['-InTiff', src]
  else:
    raise ValueError('file: %s must be TIFF or MRC'%(src))
  cmd += ['-OutMrc', dst]
  cmd += ['-Bft', args.bfactor]
  cmd += ['-Patch', args.patches, args.patches]
  cmd += ['-Kv', args.kv]
  cmd += ['-PixSize', args.apix]
  cmd += ['-Group', args.group]
  cmd += ['-LogFile', pyfs.rext(dst, '')]
  if hasattr(args, 'mocor2_norm'):
    cmd += ['-Gain', args.mocor2_norm]
  if args.binby > 1:
    cmd += ['-FtBin', args.binby]
  if args.savestack:
    cmd += ['-OutStack', 1]
  if args.expf is not None:
    cmd += ['-FmDose', args.expf]
  cmd += ['-Gpu', gpu]
  print(' '.join(map(str, cmd)))
  call(cmd)


def pbunzip2(src, dst):
  if src.endswith('.bz2'):
    with open(dst, 'wb') as fdst:
      call(['pbunzip2', src, '-c'], stdout=fdst)
  elif src.endswith('.zst'):
    call(['zstd', '-d', '-o', dst, src])
  else:
    dst = src
  return dst


def process(path, aligned, args, gpu):
  
  aligned_dw  = unpack.label(aligned, 'dw')
  aligned_log = pyfs.rext(aligned, 'shifts')
  aligned_stk = pyfs.rext(aligned, 'mrcs')

  print(path, '->', aligned)
  print(' '*len(path), '->', aligned_log)
  
  tmpdir = tempfile.mkdtemp()
  tmp_unzipped    = pyfs.join(tmpdir, 'decompressed.mrc')
  tmp_unpacked    = pyfs.join(tmpdir, 'unpacked.mrc')
  tmp_aligned     = pyfs.join(tmpdir, 'aligned.mrc')
  tmp_aligned_dw  = pyfs.join(tmpdir, 'aligned_DW.mrc')
 
  tmp_unzipped = pbunzip2(path, tmp_unzipped)
  
  if args.unpack: 
    unpack.unpack(tmp_unzipped, tmp_unpacked, args.defects, args.norm, mode='byte')
    motioncor2(tmp_unpacked, tmp_aligned, args, gpu=gpu)
  else:
    args.mocor2_norm = args.norm
    motioncor2(tmp_unzipped, tmp_aligned, args, gpu=gpu)  
  
  mv(tmp_aligned, aligned)
  mv(tmp_aligned_dw, aligned_dw)
  mvglob(pyfs.join(tmpdir, '*-Full.log'), aligned_log)
  mvglob(pyfs.join(tmpdir, '*_Stk.mrc'), aligned_stk)
  shutil.rmtree(tmpdir, False)


def mvglob(pattern, dst):
  files = tuple(pyfs.glob(pattern))
  if len(files) > 1:
    print('[warning] more than one file matches pattern: %s'%(pattern))
    mv(files[-1], dst)
  elif len(files) < 1:
    print('[warning] no files match pattern: %s'%(pattern))
  elif len(files) == 1:
    mv(files[0], dst)


def mv(src, dst):
  if pyfs.exists(src):
    try:  pyfs.mv(src, dst)
    except: pyfs.cp(src, dst)


def label(path, label):
  path = path.replace('.bz2','')
  path = path.replace('.zst','')
  path = unpack.label(path, label)
  return path

def main():
  
  args = get_arguments()
  pool = mp.Pool(args.gpus)
  results = []  
  
  gpu = 0
  for path in args.mrcs:
    dst = label(path, args.label)
    if pyfs.exists(dst):
      print(dst, 'exists, skipping', path)
      continue
    elif path.endswith('%s.mrc'%(args.label)):
      print(path, 'is not a frame stack, skipping')
      continue
    #print(path, '->', dst)
    #process(path, dst, args, gpu % args.gpus)
    results += [pool.apply_async(process, args=(path, dst, args, gpu % args.gpus))]
    gpu += 1
  pool.close()
  for result in results:
    print(result.get())
  pool.join()


if __name__ == '__main__':
  main()



