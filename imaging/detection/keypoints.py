
import math

import pyfs
import cv2

import imaging
from imaging.detection import peaks
from functional import zipmap

Keypoint = cv2.KeyPoint
FIELDS = ["x", "y", "size", "angle", "response", "octave", "class_id"]

def encode(keypoint):
	"""
	encodes a Keypoint in CSV format
	"""
	return "%f, %f, %f, %f, %f, %d, %d"%(keypoint.pt[0],
										 keypoint.pt[1],
										 keypoint.size,
										 keypoint.angle,
										 keypoint.response,
										 keypoint.octave,
										 keypoint.class_id)

def decode(line):
	"""
	decodes a Keypoint from a CSV encoded line
	"""
	types = [float, float, float, float, float, int, int]
	keys  = []
	comps = map(str.strip, line.split(","))
	if len(comps) == len(types):
		return Keypoint(*zipmap(types, comps))
	return None

def load(path):
	keypoints = []
	with open(path,"rb") as src:
		lines = src.read().split("\n")
		image_path = pyfs.join(pyfs.dpath(path), lines[0])
		for line in lines[2:]:
			keypoint = decode(line)
			if keypoint:
				keypoints += [keypoint]
	return image_path, keypoints

def save(keypoints, image_path, keypoints_path):
	if not isinstance(keypoints,list):
		keypoints = [keypoints]
	with pyfs.aopen(keypoints_path,"wb") as dst:
		# write the image path as relative to the saved keypoint file
		image_path = pyfs.relative(image_path, pyfs.dpath(keypoints_path))
		dst.write("%s\n"%image_path)
		# write column names
		columns = ", ".join(FIELDS)
		dst.write("%s\n"%columns)
		for keypoint in keypoints:
			dst.write("%s\n"%encode(keypoint))

def copy(keypoint):
	return Keypoint(keypoint.pt[0],
			        keypoint.pt[1],
			        keypoint.size,
			        keypoint.angle,
			        keypoint.response,
			        keypoint.octave,
			        keypoint.class_id)

def draw(image, keypoints, color=None):
	thickness = 1
	colors = {
		 1: imaging.rgba(255, 50, 50,255),
		 0: imaging.rgba(100,255,100,255),
		-1: imaging.rgba(100,100,255,255),
	}
	image = imaging.filters.asRGB(image)
	for keypoint in keypoints:
		if keypoint.class_id not in colors:
			colors[keypoint.class_id] = imaging.rgba.random()
		color = colors[keypoint.class_id]
		radius = keypoint.size / 2
		imaging.drawing.circle(image, keypoint.pt, radius, thickness, color)
	return image

def scale(keypoints, factor):
	def _scale(keypoint):
		keypoint = copy(keypoint)
		keypoint.pt = (keypoint.pt[0]*factor, keypoint.pt[1]*factor)
		keypoint.size = keypoint.size*factor
		keypoint.response = keypoint.response*factor
		return keypoint
	return map(_scale, keypoints)

def window(image, keypoints, extra=2.0, size=128):
	for keypoint in keypoints:
		bin = int(math.log(keypoint.size*extra, 2))+1
		col, row = keypoint.pt
		particle = imaging.filters.window(image, (row, col), 2**bin)
		particle = imaging.filters.resize(particle, size, size)
		yield bin, imaging.filters.norm(particle, 1, 1, 0.0, 1.0)

def find(pyramid, threshold, edge_threshold, psize):
	features = []
	for o, (radii, octave) in enumerate(pyramid):
		for (level, row, col), value in peaks.maxima(octave, psize):
			if value > threshold:
				s1, s2, psi = peaks.fit2d(octave[level], row, col)
				if s2 != 0 and s1 / s2 < edge_threshold:
					features += [Keypoint(col, row, radii[level], psi, value, o , 1)]
	return sorted(features, key=lambda x: x.response)
