

from pyparsing import *

Number = Word(nums+'.e-')
Number.setParseAction(lambda x: map(float, x))

uqstr = Word(alphanums)
sqstr = Suppress("'") + CharsNotIn("'") + Suppress("'")
dqstr = Suppress('"') + CharsNotIn('"') + Suppress('"')
mqstr = Suppress(';') + CharsNotIn(';') + Suppress(';')

def striplines(lines):
	stripped = []
	for line in lines.split('\n'):
		stripped += [line.strip()]
	return '\n'.join(stripped)

mqstr.setParseAction(lambda x: map(striplines, x))

String    = sqstr | dqstr | mqstr | uqstr
Comment   = Suppress('#') + Suppress(restOfLine)
CodeName  = CharsNotIn('\n\t\r ') + Optional(Comment)
DataName  = Suppress('_') + CodeName + Optional(Comment)
Keyword   = Suppress('data_') | Suppress('loop_') | Suppress('save_') | Suppress('stop_')
Value     = (~Keyword) + (~DataName) + ( Number | String ) + Optional(Comment)
Values    = Group(OneOrMore(Value))
DataItem  = Group(DataName + Value)
DataItems = OneOrMore(Comment | DataItem)

def parse_data_item(parsed):
	return { parsed[0][0]: parsed[0][1] }

def parse_data_items(parsed):
	merged = {}
	for item in parsed:
		for key in item:
			merged[key] = item[key]
	return merged

DataItem.setParseAction(parse_data_item)
DataItems.setParseAction(parse_data_items)

def encode(data):
	lines = []
	

def test():
	print DataItems.parseString('''
		_a 1 _b 2
		_c 3 # now
		# test
		_d 6
		''')

if __name__ == '__main__':
	test()




