

class defer(object):
    '''
    meant to be used for lazy evaluation of an object attribute.
    property should represent non-mutable data, as it replaces itself.
    '''
    
    def __init__(self, fget):
        self.fget = fget
    
    
    def __get__(self, obj, cls):
        value = self.fget(obj)
        setattr(obj, self.fget.func_name, value)
        return value
    


