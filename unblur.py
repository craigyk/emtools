#!/home/exacloud/lustre1/users/yoshiokc/sw/cky-tools/site/bin/python

import os
import sh
import pyfs

import numpy as np
from normalize import normalize

def bunzip2(srcpath, dstpath):
    if os.system('pbunzip2 -p8 -kc %s > %s' % (srcpath, dstpath)) != 0:
        raise RuntimeError('could not bunzip2 file: %s' % (srcpath))
    return dstpath


def bzip2(srcpath, dstpath):
    os.system('pbzip2 -p8 -kc %s > %s' % (srcpath, dstpath))
    return dstpath


def gzip(srcpath, dstpath):
    if os.system('gzip -nc %s > %s' % (srcpath, dstpath)) != 0:
        raise RuntimeError('could not gzip file: %s' % (srcpath))
    return dstpath


def gunzip(srcpath, dstpath):
    os.system('gunzip -nc %s > %s' % (srcpath, dstpath))
    return dstpath


def mkdtemp():
    import tempfile
    return tempfile.mkdtemp(prefix='unblur')


def resolve(path):
    if path is None:
        return None
    return pyfs.resolve(path)


def unblur(frames_path, pixel_size, kv=None, 
                                    aligned_sum_path=None,
                                    aligned_shifts_path=None,
                                    norm_path=None,
                                    aligned_frc_path=None,
                                    aligned_frames_path=None, 
                                    exposure_per_frame=None,
                                    pre_exposure_dose=0.0, 
                                    bfactor=2500, 
                                    iterations=20, 
                                    max_shift=100.0,
                                    initial_shift=3.0):


    # use full pathnames, because we will be running
    # unblur from a temp directory to prevent race 
    # conditions with the .Unblur temp files it generates

    def loadit(src, dst):
        if src.endswith('.bz2'):
            print('decompressing:\n  %s -> %s' % (src, dst))
            return bunzip2(src, dst)
        elif src.endswith('.gz'):
            print('decompressing:\n  %s -> %s' % (src, dst))
            return gunzip(src, dst)
        else:
            print('linking:\n  %s -> %s' % (src, dst))
            return pyfs.lns(src, dst)

    def saveit(src, dst):
        if src is None or dst is None:
            return None
        try:
            if dst.endswith('.bzip2'):
                print('compressing:\n  %s -> %s' % (src, dst))
                return bzip2(src, dst)
            elif dst.endswith('.gz'):
                print('compressing:\n  %s -> %s' % (src, dst))
                return gzip(src, dst)
            else:
                tmppath = dst + '.tmp'
                print('saving:\n  %s -> %s' % (src, dst))
                pyfs.cp(src, tmppath)
                pyfs.mv(tmppath, dst)
                return dst
        except Exception as e:
            print('[error]:', e)

    frames_path = resolve(frames_path)
    aligned_frames_path = resolve(aligned_frames_path)
    aligned_shifts_path = resolve(aligned_shifts_path)
    aligned_sum_path = resolve(aligned_sum_path)
    aligned_frc_path = resolve(aligned_frc_path)

    if pyfs.exists(aligned_sum_path):
        print('  [warning] aligned frames already exist, skipping')
        return 

    def unblurit(tmpdir, frames_path):

        DEFAULT = ''
        YES = 'YES'
        NO = 'NO'
        NULL = '/dev/null'
        EOF = ''

        def tempit(path):
            if path is None:
                return None
            return pyfs.join(tmpdir, path)

        _frames_path = loadit(frames_path, tempit('input.mrc'))
        _aligned_sum_path = tempit('aligned.mrc')
        _aligned_frames_path = tempit('aligned.frames.mrc')
        _aligned_shifts_path = tempit('aligned.shifts')
        _aligned_frc_path = tempit('aligned.frc')   

        if norm_path is not None:
            print('normalizing image:\n  %s -> %s' % (frames_path, tempit('input.normed.mrc')))
            _frames_path = normalize(_frames_path, norm_path, tempit('input.normed.mrc'))

        input  = []
        input += [_frames_path]
        input += [DEFAULT]                   # default: use all frames
        input += [_aligned_sum_path]
        input += [_aligned_shifts_path]
        input += ['%.3f' % (pixel_size)]
        
        if exposure_per_frame is not None:
            input += [YES] 
            input += ['%.2f' % (exposure_per_frame)]
            if kv is None:
                raise ValueError('need to be given kv param if doing dose compensation')
            input += ['%f' % (kv)]
            input += ['%f' % (pre_exposure_dose)]
        else:
            input += [NO]
        
        if aligned_frames_path is not None:
            input += [YES]
            input += [_aligned_frames_path]
        else:
            input += [NO]
        
        input += [YES]                      # set expert options
        input += [_aligned_frc_path]        # output FRC file
        input += ['%f' % (initial_shift)]   # minimum initial shift resolution
        input += ['%f' % (max_shift)]
        input += ['%f' % (bfactor)]
        input += ['1']                      # half-width of central vertical line in Fourier mask
        input += ['1']                      # half-width of central horizontal line in Fourier mask
        input += ['0.1']                    # termination threshold for shifts (angstroms)
        input += ['%d' % (iterations)]

        if exposure_per_frame is not None:
            input += [YES]                  # restore noise power
        
        input += [NO]                       # verbose output
        input += [EOF]                      # new line to end file
        input = '\n'.join(input)

        print('unblurring frames:\n  %s' % (_frames_path))
        print('   aligned average -> %s' % (_aligned_sum_path))
        print('    aligned frames -> %s' % (_aligned_frames_path))
        print('            shifts -> %s' % (_aligned_shifts_path))
        sh.unblur(_in=input, _cwd=tmpdir)
        shifts = get_shifts(_aligned_shifts_path)
        print('  mean shifts:', np.sum(shifts))
	
        saveit(_aligned_frc_path, aligned_frc_path)
        saveit(_aligned_sum_path, aligned_sum_path)
        saveit(_aligned_frames_path, aligned_frames_path)
        saveit(_aligned_shifts_path, aligned_shifts_path)

    try:
        tmpdir = mkdtemp()
        return unblurit(tmpdir, frames_path)
    except Exception as e:
        print('error unblurring: %s in cwd %s' % (frames_path, tmpdir))
        raise
    finally:
        pyfs.rmtree(tmpdir)

def get_shifts(path):
    coords = []
    with open(path, 'rb') as src:
        for line in src:
            try:
                coords += [list(map(float, line.split()))]
            except ValueError as e:
                pass
    shifts = np.diff(coords, axis=1)
    shifts = np.linalg.norm(shifts, axis=0)
    return shifts    


