

class defer(object):

    '''
    meant to be used as a decorator for lazy evaluation of an object attribute.
    property value needs to be non-mutable data, as it replaces itself.
    '''

    def __init__(self, fget):
        self.fget = fget

    def __get__(self, obj, cls):
        value = self.fget(obj)
        setattr(obj, self.fget.__name__, value)
        return value
