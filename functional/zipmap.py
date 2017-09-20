
def zipmap(funcs, items):
	for func, item in zip(funcs, items):
		yield func(item)
