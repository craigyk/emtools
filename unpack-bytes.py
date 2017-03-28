#!/usr/bin/env cky-python

import os
import sys
import multiprocessing as mp


def get_arguments():
    import argparse

    parser = argparse.ArgumentParser(
        description='unpacks byte-packed SerialEM images and optionally applies DM normalizations')

    parser.add_argument('-i', '--mrcs', required=True, nargs='+', help='paths to SerialEM images')
    parser.add_argument('-d', '--defects', default=None,
                        help='Digital Micrograph defects file')
    parser.add_argument('-n', '--norm', default=None,
                        help='Digital Micrograph normalization file')
    parser.add_argument('-l', '--label', default='unpacked',
                        help='label to attach to unpacked images')
    return parser.parse_args()


def unpack(mrc, label, defects, norm):
  name, ext = os.path.splitext(mrc)
  if name.endswith('_%s'%(label)):
    print('ignoring:', mrc)
    return
  unpacked = '%s_%s.mrc'%(name, label)
  if os.path.exists(unpacked):
    print('skipping:', mrc)
    return
  tmp = '%s.tmp'%(unpacked)
  cmd = ['clip', 'unpack', '-n', 14, '-m', 0]
  if defects is not None:
    cmd += ['-D', defects, '-S']
  cmd += [mrc]
  if norm is not None:
    cmd += [norm]
  cmd += [tmp]
  cmd = ' '.join([str(x) for x in cmd])
  print('unpacking:', mrc)
  os.system(cmd)
  os.rename(tmp, unpacked)
  print(mrc, '->', unpacked)


def main():
  
  args = get_arguments()
  procs = int(os.environ.get('SLURM_JOB_CPUS_PER_NODE', default='1'))
  
  done = 0
  def progress(_):
    nonlocal done
    done += 1
    percent = ( float(done) / float(len(args.mrcs)) ) * 100.0
    print('---- %.2f%% done'%(percent))
  
  pool = mp.Pool(procs)
  for mrc in args.mrcs:
    pool.apply_async(unpack, args=(mrc, args.label, args.defects, args.norm), callback=progress)
  pool.close()
  pool.join()


if __name__ == '__main__':
  main()


