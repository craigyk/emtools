#!/usr/bin/env cky-python

import os
import unpack
import multiprocessing as mp

def main():
  
  args = unpack.get_arguments()
  procs = int(os.environ.get('SLURM_JOB_CPUS_PER_NODE', default='1'))
  
  pool = mp.Pool(procs)
  for mrc in args.mrcs:
    dst = unpack.label(mrc, args.label)
    if unpack.pyfs.exists(dst):
      continue
    #unpack.unpack(mrc, dst, args.defects, args.norm, args.mode)
    pool.apply_async(unpack.unpack, args=(mrc, dst, args.defects, args.norm, args.mode))
  pool.close()
  pool.join()


if __name__ == '__main__':
  main()


