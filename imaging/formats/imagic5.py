#!/usr/bin/env python

import pyfs
import numpy as np

NAMES = ["hed", "img"]

def UNUSED_WORDS(count):
	UNUSED_WORDS.counter += 1
	return ("UNUSED_%d"%UNUSED_WORDS.counter,"%df4"%count)
UNUSED_WORDS.counter = 0

IMAGIC4DHeader = np.dtype([
	("IMN","i4"),
	("IFOL","i4"),
	("IERROR","i4"),
	("NBLOCKS","i4"),
	("DATE","6i4"),
	("RSIZE","i4"),
	("IZOLD","i4"),
	("ROWS","i4"),
	("COLS","i4"),
	("TYPE","a4"),
	("IXOLD","i4"),
	("IYOLD","i4"),
	("AVDENS","f4"),
	("SIGMA","f4"),
	("USER1","f4"),
	("USER2","f4"),
	("DENSMAX","f4"),
	("DENSMIN","f4"),
	("COMPLEX","i4"),
	("DEFOCUS1","f4"),
	("DEFOCUS2","f4"),
	("DEFANGLE","f4"),
	("SINOSTART","f4"),
	("SINOEND","f4"),
	("NAME","a80"),
	("SECS","i4"),
	("I4LP","i4"),
	("I5LP","i4"),
	("I6LP","i4"),
	("ABG1","3f4"),
	("IMAVERS","i4"),
	("REALTYPE","i4"),
	("BUFFCTRL","29i4"),
	("ANGLE","f4"),
	("VOLTAGE","f4"),
	("SPABERR","f4"),
	("FOCDIST","f4"),
	("CCC","f4"),
	("ERRAR","f4"),
	("ERR3D","f4"),
	("REF","i4"),
	("CLASSNO","f4"),
	("LOCOLD","f4"),
	("REPQUAL","f4"),
	("ZSHIFT","f4"),
	("XSHIFT","f4"),
	("YSHIFT","f4"),
	("NUMCLS","f4"),
	("OVQUAL","f4"),
	("EANGLE","f4"),
	("EXSHIFT","f4"),
	("EYSHIFT","f4"),
	("CMTOTVAR","f4"),
	("INFORMAT","f4"),
	("NUMEIGEN","i4"),
	("NIACTIVE","i4"),
	("RESOL","f4"),
	UNUSED_WORDS(2),
	("ABG2","3f4"),
	("NMETRIC","f4"),
	("ACTMSA","f4"),
	("COOSMSA","68f4"),
	("HISTORY","a228"),
	]) 

IMAGIC5Header = np.dtype([
	("IMN","i4"),
	("IFOL","i4"),
	("IERROR","i4"),
	("NHFR","i4"),
	("DATE","6i4"),
	("NPIX2","i4"),
	("NPIXEL","i4"),
	("ROWS","i4"),
	("COLS","i4"),
	("TYPE","a4"),
	("IXOLD","i4"),
	("IYOLD","i4"),
	("AVDENS","f4"),
	("SIGMA","f4"),
	("VARIAN","f4"),
	("OLDAVD","f4"),
	("DENSMAX","f4"),
	("DENSMIN","f4"),
	("COMPLEX","i4"),
	("CLENGTH","3f4"),
	("CALPHA","f4"),
	("CBETA","f4"),
	("NAME","a80"),
	("CGAMMA","f4"),
	("MAPC","i4"),
	("MAPR","i4"),
	("MAPS","i4"),
	("ISPG","i4"),
	("START","3i4"),
	("INTV","3i4"),
	("IZLP","i4"),
	("I4LP","i4"),
	("I5LP","i4"),
	("I6LP","i4"),
	("ABG1","3f4"),
	("IMAVERS","i4"),
	("REALTYPE","i4"),
	("BUFFCTRL","29i4"),
	("RONLY","i4"),
	("ANGLE","f4"),
	("RCP","f4"),
	("IPEAK","2i4"),
	("CCC","f4"),
	("ERRAR","f4"),
	("ERR3D","f4"),
	("REF","i4"),
	("CLASSNO","f4"),
	("LOCOLD","f4"),
	("OLDAVD2","f4"),
	("OLDSIGMA","f4"),
	("SHIFT","2f4"),
	("NUMCLS","f4"),
	("OVQUAL","f4"),
	("EANGLE","f4"),
	("ESHIFT","2f4"),
	("CMTOTVAR","f4"),
	("INFORMAT","i4"),
	("NUMEIGEN","i4"),
	("NIAXCTIVE","f4"),
	("RESOL","3f4"),
	("ABG2","3f4"),
	("NMETRIC","f4"),
	("ACTMSA","f4"),
	("COOSMSA","69f4"),
	("HISTORY","a228"),
	]) 

def readHeader(fd):
	header = np.fromfile(fd,dtype=IMAGIC5Header,count=1)[0]
	if not headerIsSane(header):
		header = header.byteswap()
		if not headerIsSane(header):
			raise ValueError("file is not a valid IMAGIC header")
	if not checkHeader(header):
		raise ValueError("type of IMAGIC file is not currently supported")
	return header

def headerIsSane(header):
	return header["TYPE"] in ["REAL","INTG","PACK","COMP","RECO"]

def checkHeader(header):
	if header["TYPE"] not in ["REAL"]:
		raise ValueError("type if not supported")
	return True

def show(header):
	print("-------------------")
	for field in header.dtype.names:
		if "UNUSED" not in field:
			offset = header.dtype.fields[field][1] / 4 + 1
			print("%d: %s: %s"%(offset,field,header[field]))
	print("-------------------")
	return header

def read(path):
	with open(pyfs.rext(path,"hed"),"rb") as fd:
		header = readHeader(fd)
	with open(pyfs.rext(path,"img"),"r+b") as fd:
		rows = int(header["ROWS"])
		cols = int(header["COLS"])
		count = int(header["IFOL"]+1)
		data = np.memmap(fd,dtype="f4",mode="c")
		return data.reshape([count,cols,rows])

def save(data,path):
	raise NotImplemented

if __name__ == "__main__":
	import sys
	import spider
	import filters
	stack = read(sys.argv[1])
	for i, image in enumerate(stack):
		image = filters.resize(image,[32,32])
		spider.save(image,"particles/%d.spi"%i)
	

