#!/usr/bin/env cky-python

import os
import sys
import pyfs
import time
import math
import shutil
import imaging
import tempfile
import subprocess as sp
import multiprocessing as mp

import unpack

def call(args, stderr=sp.DEVNULL, stdout=sp.DEVNULL):
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
  parser.add_argument('-e', '--correction', default=None, type=float,
                        help='electrons per angstrom per frame correction (from header) if dose compensation is desired')
  parser.add_argument('-b', '--bfactor', default=100.0, type=float,
                        help='bfactor used for alignment')
  parser.add_argument('--gpus', default=1, type=int,
                        help='number of gpus to run on')
  parser.add_argument('--group', default=1, type=int,
                        help='number of frames to group to improve SNR')
  parser.add_argument('--inframe', default=False, action='store_true',
                        help='use in-frame movement weighting')
  parser.add_argument('--patches', default=5, type=int,
                        help='number of patches for local refinement')
  parser.add_argument('-u', '--unpack', default=False, action='store_true',
                        help='unpack byte-packed frames')
  parser.add_argument('-s', '--savestack', default=False, action='store_true',
                        help='save aligned stack')
  parser.add_argument('--binby', default=1, type=int,
                        help='binning by motioncor2')
  parser.add_argument('--mag', default=None, type=float, nargs=3,
                        help='mag correction factor: major, minor, angle (from mag_distortion_estimate)')
  parser.add_argument('--rotgain', action='store_true',
                        help='rotate gain')
  parser.add_argument('--throw', type=int, default=0,
                        help='remove this number of starting frames')
  parser.add_argument('--truncate', type=int, default=0,
                        help='remove this number of ending frames')
  return parser.parse_args()


def calculate_dose(mrcpath, apix, correction=1.0):
  mean = imaging.FORMATS['mrc'].load_header(mrcpath)['mean']
  if mean <= 0.0:
    return correction
  return correction * (mean / (apix*apix))


def calculate_grouping(mrcpath, grouping):
  header = imaging.FORMATS['mrc'].load_header(mrcpath)
  return min(7, math.ceil( grouping / header['mean']))


def motioncor2(src, dst, args, gpu=0):
  print('processing: %s'%(src))
  cmd  = ['motioncor2']
  if src.endswith('.mrc') or src.endswith('.mrcs'):
    cmd += ['-InMrc', src]
  elif src.endswith('.tif'):
    cmd += ['-InTiff', src]
  else:
    raise ValueError('file: %s must be TIFF or MRC'%(src))
  cmd += ['-OutMrc', dst]
  cmd += ['-Bft', args.bfactor]
  cmd += ['-Patch', args.patches, args.patches, 50]
  cmd += ['-Kv', args.kv]
  cmd += ['-PixSize', args.apix]
  cmd += ['-LogFile', pyfs.rext(dst, '')]
  if args.inframe:
    cmd += ['-InFmMotion', 1]
  if args.mag is not None:
    print(args.mag)
    cmd += ['-Mag', args.mag[0], args.mag[1], args.mag[2]]
  if hasattr(args, 'mocor2_norm'):
    cmd += ['-Gain', args.mocor2_norm]
  if args.binby > 1:
    cmd += ['-FtBin', args.binby]
  if args.savestack:
    cmd += ['-OutStack', 1]
  if args.throw:
    cmd += ['-Throw', args.throw]
  if args.truncate:
    cmd += ['-Trunc', args.truncate]
  if args.correction is not None:
    expf = calculate_dose(src, args.apix, args.correction)
    print('using dose rate: %f e-/A^2/frame'%(expf))
    cmd += ['-FmDose', expf]
  group = calculate_grouping(src, args.group)
  cmd += ['-Group', args.group]
  if args.rotgain:
    cmd += ['-RotGain', 3, '-FlipGain', 1]
  cmd += ['-Gpu', gpu]
  print(' '.join(map(str, cmd)))
  call(cmd, stdout=None)


def pbunzip2(src, dst):
  if src.endswith('.bz2'):
    with open(dst, 'wb') as fdst:
      call(['pbunzip2', src, '-c'], stdout=fdst)
  elif src.endswith('.zst'):
    print('zstd', '-dv', '-o', dst, src)
    call(['zstd', '-dv', '-o', dst, src], stdout=None, stderr=None)
  else:
    dst = src
  return dst


def process(path, aligned, args, gpu):
  
  t0 = time.time()
  
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
  tmp_logfile     = pyfs.join(tmpdir, 'aligned0-Full.log')
  tmp_stkfile     = pyfs.join(tmpdir, 'aligned_Stk.mrc') 
 
  tmp_unzipped = pbunzip2(path, tmp_unzipped)
  
  if args.unpack: 
    unpack.unpack(tmp_unzipped, tmp_unpacked, args.defects, args.norm, mode='byte')
    motioncor2(tmp_unpacked, tmp_aligned, args, gpu=gpu)
  else:
    args.mocor2_norm = args.norm
    motioncor2(tmp_unzipped, tmp_aligned, args, gpu=gpu)  

  mv(tmp_aligned, aligned)
  mv(tmp_aligned_dw, aligned_dw)
  mv(tmp_logfile, aligned_log)
  mv(tmp_stkfile, aligned_stk)
  shutil.rmtree(tmpdir, False)
  
  print('aligning: %s took: %.2f secs'%(path, time.time()-t0))


def mv(src, dst):
  if pyfs.exists(src):
    try:  pyfs.mv(src, dst)
    except: pyfs.cp(src, dst)


def label(path, label):
  path = path.replace('.bz2','')
  path = path.replace('.zst','')
  path = unpack.label(path, label)
  return path


def workerit(queue, args, gpuid):
  while True:
    src, dst = queue.get()
    if src is None:
      return
    process(src, dst, args, gpuid)
    queue.task_done()


def main():
  
  args = get_arguments()
  #pool = mp.Pool(args.gpus)
  queue = mp.JoinableQueue()
  workers = [mp.Process(target=workerit, args=(queue, args, gpuid)) for gpuid in range(args.gpus)]
  for worker in workers:
    worker.start()
  
  #results = []  
  #
  #gpu = 0
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
    #results += [pool.apply_async(process, args=(path, dst, args, gpu % args.gpus))]
    #gpu += 1
    queue.put((path, dst))
  #pool.close()
  #for result in results:
    #print(result.get())
  #pool.join()
  queue.join()
  for worker in workers:
    queue.put((None, None))
    



if __name__ == '__main__':
  main()



