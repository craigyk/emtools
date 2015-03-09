
from pyparsing import *

from loop import Loop
from frame import Frame
from values import Value, Comment, Keyword, DataName, CodeName, DataItems

Start = Suppress('data_') + Optional(CodeName, None)

Data = Start + OneOrMore(DataItems | Loop | Frame)

def norm(parsed):
	name = 'data_%s'%(parsed[0] or '')
	values = {}
	for item in parsed[1:]:
		for key in item:
			values[key] = item[key]
	return { name: values }

Data.setParseAction(norm)

def test():
	print Data.parseString('''
		data_name
		_a 2
		save_sdfsdaf
		_a 3
		_stop
		save_sadfsdf
		_a 3
		''')

if __name__ == '__main__':
	test()

