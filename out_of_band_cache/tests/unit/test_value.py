"""Unit tests for the out-of-band cache Value replacement."""

import unittest

import pmock

import time

import out_of_band_cache
from beaker.synchronization import NameLock

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
    
    def testCreateNewValue(self):
        """Test creating a new value."""
        mock_namespace = SubscriptableMock()
        mock_namespace.expects(pmock.at_least_once()).acquire_read_lock()
        mock_namespace.expects(pmock.at_least_once()).release_read_lock()
        mock_namespace.expects(pmock.at_least_once()).has_key(
            pmock.eq(self.test_key)).will(pmock.return_value(False))
        mock_namespace.expects(pmock.once()).acquire_write_lock()
        mock_namespace.expects(pmock.once()).release_write_lock()
        create_lock = NameLock(None, False)
        mock_namespace.expects(pmock.once()).get_creation_lock(
            pmock.eq(self.test_key)).will(pmock.return_value(create_lock))
        mock_namespace.expects(pmock.once()).method('set_value')
        def mock_create_func():
            return "new_value"
        value = out_of_band_cache.Value(self.test_key, mock_namespace,
                                        createfunc=mock_create_func).get_value()
        self.assertEqual(value, 'new_value')
        mock_namespace.verify()
    
    def testCreateNewValue(self):
        """Test creating a new value."""
        mock_namespace = SubscriptableMock()
        mock_namespace.expects(pmock.at_least_once()).acquire_read_lock()
        mock_namespace.expects(pmock.at_least_once()).release_read_lock()
        mock_namespace.expects(pmock.at_least_once()).has_key(
            pmock.eq(self.test_key)).will(pmock.return_value(False))
        mock_namespace.expects(pmock.once()).acquire_write_lock()
        mock_namespace.expects(pmock.once()).release_write_lock()
        create_lock = NameLock(None, False)
        mock_namespace.expects(pmock.once()).get_creation_lock(
            pmock.eq(self.test_key)).will(pmock.return_value(create_lock))
        mock_namespace.expects(pmock.once()).method('set_value')
        def mock_create_func():
            return "new_value"
        value = out_of_band_cache.Value(self.test_key, mock_namespace,
                                        createfunc=mock_create_func).get_value()
        self.assertEqual(value, 'new_value')
        mock_namespace.verify()
    
    def testReplaceExistingValue(self):
        """Test replacing an existing value."""
        class TestNamespace(dict):
            def __init__(self):
                self.creation_lock = NameLock(None, False)
            
            def acquire_read_lock(self):
                pass
            
            def release_read_lock(self):
                pass
            
            def acquire_write_lock(self):
                pass
            
            def release_write_lock(self):
                pass
            
            def get_creation_lock(self, key):
                return self.creation_lock
            
            def set_value(self, key, value):
                self[key] = value
                
        def mock_create_func():
            return "new_value"
        namespace = TestNamespace()
        namespace[self.test_key] = (0, 0, 'old_value')
        value = out_of_band_cache.Value(self.test_key, namespace,
                                        createfunc=mock_create_func,
                                        expiretime=5000)
        self.assertEqual(value.has_value(), True)
        self.assertEqual(value.get_value(), 'old_value')
        namespace.creation_lock.acquire() # Wait for worker thread to finish.
        self.assertEqual(value.get_value(), 'new_value')
