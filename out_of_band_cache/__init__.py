class NewValueInProgressException(Exception):
    """Raised when a request is made for a value we don't have in the cache."""
    pass
