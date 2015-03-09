#!/usr/bin/env python

from pyparsing import *
from data import Data

Document = OneOrMore(Data)

def test():
	print Document.parseString('''
		data_
		save_
		_a 2
		_c 4
		loop_
		_a
		_b
		_c
		1 2 3
		4 5 6
		7 8 9
		loop_
		_d
		_e
		_f
		1 2 3
		4 5 6
		data_abc
		_a 1
		''')

def load(path):
	with open(path, 'r') as src:
		return Document.parseString(src.read())

def save(data, path):
	pass

if __name__ == '__main__':
	test()
	print load('relion_particles.star')

