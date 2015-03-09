
from pyparsing import *

from loop import Loop
from values import Value, Comment, Keyword, DataName, CodeName, DataItems

Start = Suppress('save_') + Optional(CodeName, None)

Frame  = Start + OneOrMore(Loop | DataItems)

def norm(parsed):
	name = 'frame_%s'%(parsed[0] or id(parsed))
	values = {}
	for item in parsed[1:]:
		if isinstance(item, list):
			values[tuple(item[0])] = item[1:]
		elif isinstance(item, dict):
			for key in item:
				values[key] = item[key]
	return { name: values }
		
Frame.setParseAction(norm)

def test():
	print Frame.parseString('''
		save_name
		_x 1
		_y 2
		loop_
		_a
		_b
		1 2
		3 4
		5 6
		loop_
		_c
		_d
		1 2
		3 4
		_k 1
		save_
		_a 1
		''')

if __name__ == '__main__':
	test()
