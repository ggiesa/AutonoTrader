class Error(Exception):
    '''Base class for all custom exceptions.'''
    pass

class DiscontinuousError(Error):
    '''Used to signal discontinuous datetime sequences.'''
    pass

class ImplementationError(Error):
    '''Used to signal that user has implemented something incorrectly.'''
    pass
