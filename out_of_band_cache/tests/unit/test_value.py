"""Unit tests for the out-of-band cache Value replacement."""

import unittest

import pmock

import time

import out_of_band_cache

class SubscriptableMock(pmock.Mock):
    """A mock object that can be subscripted."""
    def __getitem__(self, key):
        return self.mock__getitem__(key)

class TestValue(unittest.TestCase):
    def setUp(self):
        self.test_key = 'test_key'
    
    def testGetExistingValue(self):
        """Test getting an existing value."""
        mock_namespace = SubscriptableMock()
        mock_namespace.expects(pmock.at_least_once()).acquire_read_lock()
        mock_namespace.expects(pmock.at_least_once()).release_read_lock()
        mock_namespace.expects(pmock.once()).has_key(
            pmock.eq(self.test_key)).will(pmock.return_value(True))
        mock_namespace.expects(pmock.once()).mock__getitem__(
            pmock.eq(self.test_key)).will(pmock.return_value(
            (time.time() - 60*5, time.time() + 60*5, 42)))
        def mock_create_func():
            self.fail('Attempted to create a new value.')
        value = out_of_band_cache.Value(self.test_key, mock_namespace,
                                        createfunc=mock_create_func).get_value()
        mock_namespace.verify()
