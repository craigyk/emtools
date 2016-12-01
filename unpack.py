#!/usr/bin/env cky-python

import os
import sys
import pyfs
import tempfile
import multiprocessing as mp



MODES = { 'byte'  : 0,
          'float' : 2, }


MODE_SCALING = { 'byte'  : 14, # added headroom, 14 instead of 16 to prevent integer overflow after gain norm
                 'float' :  1,
               }


def get_arguments():
  
  import argparse
    
  def mrcmode(x):
    if x not in MODES:
      raise argparse.ArgumentTypeError('mode must be one of %s'%(str(MODES.keys())))
    return x

  parser = argparse.ArgumentParser(
    description='unpacks byte-packed SerialEM images and optionally applies DM normalizations')

  parser.add_argument('-i', '--mrcs', nargs='+', help='paths to SerialEM images')
  parser.add_argument('-d', '--defects', default=None,
                        help='Digital Micrograph defects file')
  parser.add_argument('-n', '--norm', default=None,
                        help='Digital Micrograph normalization file')
  parser.add_argument('-l', '--label', default='unpacked',
                        help='label to attach to unpacked images')
  parser.add_argument('-m', '--mode', default='byte', type=mrcmode,
                        help='format of values in unpacked image stack ( byte, float )')
  return parser.parse_args()



def unpack(srcmrc, dstmrc, defects=None, norm=None, mode='byte'):
  tmpmrc = dstmrc + '.tmp.mrc'
  cmd = ['clip', 'unpack']
  cmd += ['-n', MODE_SCALING[mode]]
  cmd += ['-m', MODES[mode]]
  if defects is not None:
    cmd += ['-D', defects, '-S']
  cmd += [srcmrc]
  if norm is not None:
    cmd += [norm]
  cmd += [tmpmrc]
  cmd = ' '.join([str(x) for x in cmd])
  print(cmd)
  os.system(cmd)
  os.rename(tmpmrc, dstmrc)
  print(srcmrc, '->', dstmrc)


def label(path, label):
  parts = pyfs.split(path)
  filename, ext = pyfs.sext(parts[-1])
  final_filename = '%s_%s.%s' % (filename, label, ext)
  return pyfs.join(*parts[:-1], final_filename)
 
  


