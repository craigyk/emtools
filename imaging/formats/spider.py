#!/usr/bin/env python

import numpy as np

from imaging import filters

NAMES = ["spi", "xmp"]

def lenbyt(cols):
	return int(cols) * 4

def labrec(cols):
	labrec = int(256/int(cols)) + 1
	if labrec == 0:
		labrec += 1
	return labrec

def labbyt(cols):
	return lenbyt(cols) * labrec(cols)

def UNUSED_CARDS(count):
	UNUSED_CARDS.counter += 1
	return ("UNUSED_%d"%UNUSED_CARDS.counter,"%df4"%count)
UNUSED_CARDS.counter = 0

SpiderHeader = np.dtype([
	("NZ","f4"),
	("NY","f4"),
	("IREC","f4"),
	UNUSED_CARDS(1),
	("IFORM","f4"),
	("IMAMI","i4"),
	("FMAX","f4"),
	("FMIN","f4"),
	("AV","f4"),
	("SIG","f4"),
	UNUSED_CARDS(1),
	("NX","f4"),
	("LABREC","f4"),
	("IANGLE","i4"),
	("PHI","f4"),
	("THETA","f4"),
	("GAMMA","f4"),
	("XOFF","f4"),
	("YOFF","f4"),
	("ZOFF","f4"),
	("SCALE","f4"),
	("LABBYT","f4"),
	("LENBYT","f4"),
	("ISTACK","i4"),
	UNUSED_CARDS(1),
	("MAXIM","f4"),
	("IMGNUM","f4"),
	("LASTINDX","f4"),
	UNUSED_CARDS(2),
	("KANGLE","f4"),
	("PHI1","f4"),
	("THETA1","f4"),
	("PSI1","f4"),
	("PHI2","f4"),
	("THETA2","f4"),
	("PSI2","f4"),
	("PIXSIZ","f4"),
	("EV","f4"),
	("PROJ","f4"),
	("MIC","f4"),
	("NUM","f4"),
	UNUSED_CARDS(49-43),
	("TRANSFORMS","%df4"%(76-50)),
	UNUSED_CARDS(100-77),
	("PSI3","f4"),
	("THETA3","f4"),
	("PHI3","f4"),
	("LANGLE","f4"),
	UNUSED_CARDS(211-105),
	("CDAT","a%d"%(214-212)),
	("CTIM","a%d"%(216-215)),
	("CTIT","a%d"%(256-217))
	]) 

def readHeader(fd):
	header = np.fromfile(fd,dtype=SpiderHeader,count=1)[0]
	if not headerIsSane(header):
		header = header.byteswap()
		if not headerIsSane(header):
			raise ValueError("file is not a spider image")
	return header

def headerIsSane(header):
	return int(header["LABBYT"]) == labbyt(header["NX"]) 

def show(header):
	print("-------------------")
	for field in header.dtype.fields:
		if "UNUSED" not in field:
			print("%s: %s"%(field,header[field]))
	print("-------------------")
	return header

def load(path,norm=False,supress=False):
	with open(path,"rb") as fd:
		header = readHeader(fd)
		fd.seek(int(header["LABBYT"]))
		rows = int(header["NX"])
		cols = int(header["NY"])
		if int(header["NZ"]) != 1:
			raise ValueError("spider volumes not supported")
		if int(header["ISTACK"]) != 0:
			raise ValueError("spider stacks not supported")
		data = np.fromfile(fd,dtype="f4")
		if data.size != rows*cols:
			raise ValueError("spider image has bad/incomplete data")
		if supress:
			data = filters.supress(data,*supress)
		if norm:
			data = filters.norm(data,*norm)
		return data.reshape([cols,rows])

def formHeader(image,compute_stats=False):

	if (image.ndim,image.dtype.kind,image.dtype.itemsize) != (2,"f",4):
		raise ValueError("only spider simple images are supported")	
	
	header = np.zeros(1,dtype=SpiderHeader)[0]
	
	header["IFORM"]  = 1
	header["NZ"]     = 1
	header["NY"]     = image.shape[-2]
	header["NX"]     = image.shape[-1]
	header["SCALE"]  = 1
	header["LABREC"] = labrec(image.shape[-1])
	header["LABBYT"] = labbyt(image.shape[-1])
	header["LENBYT"] = lenbyt(image.shape[-1])

	header["IMAMI"] = 0
	header["SIG"]   = -1
	if compute_stats:
		header["IMAMI"] = 1
		header["SIG"] = np.std(image)
		header["FMAX"] = np.max(image)
		header["FMIN"] = np.min(image)
		header["AV"]   = np.mean(image)

	return header

def save(image, path, compute_stats=False, **kwargs):
	image = image.astype("f4")
	with open(path,"wb") as fd:
		header = formHeader(image,compute_stats=compute_stats)
		header.tofile(fd)
		fd.seek(int(header["LABBYT"]))
		image.tofile(fd)
		return path

