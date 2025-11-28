"""Tests for DatetimeWithNanoseconds functionality."""
import unittest
from datetime import datetime, timezone
from mockfirestore import MockFirestore, DatetimeWithNanoseconds


class TestDatetimeWithNanoseconds(unittest.TestCase):
    """Test DatetimeWithNanoseconds class and conversion."""

    def setUp(self):
        """Set up test fixtures."""
        self.db = MockFirestore()

    def test_datetime_conversion_simple(self):
        """Test that regular datetime is converted to DatetimeWithNanoseconds."""
        now = datetime.now()

        # Store a document with a datetime
        self.db.collection('test').document('doc1').set({
            'created_at': now
        })

        # Retrieve and check type
        doc = self.db.collection('test').document('doc1').get()
        retrieved_datetime = doc.to_dict()['created_at']

        self.assertIsInstance(retrieved_datetime, DatetimeWithNanoseconds)
        self.assertTrue(hasattr(retrieved_datetime, 'nanosecond'))
        self.assertIsInstance(retrieved_datetime.nanosecond, int)
        self.assertGreaterEqual(retrieved_datetime.nanosecond, 0)
        self.assertLessEqual(retrieved_datetime.nanosecond, 999999999)

    def test_datetime_conversion_preserves_values(self):
        """Test that datetime values are preserved during conversion."""
        original_datetime = datetime(2023, 11, 28, 14, 30, 45, 123456)

        self.db.collection('test').document('doc1').set({
            'timestamp': original_datetime
        })

        doc = self.db.collection('test').document('doc1').get()
        retrieved = doc.to_dict()['timestamp']

        # Check all datetime components are preserved
        self.assertEqual(retrieved.year, 2023)
        self.assertEqual(retrieved.month, 11)
        self.assertEqual(retrieved.day, 28)
        self.assertEqual(retrieved.hour, 14)
        self.assertEqual(retrieved.minute, 30)
        self.assertEqual(retrieved.second, 45)
        self.assertEqual(retrieved.microsecond, 123456)

        # Nanoseconds should be microseconds * 1000
        self.assertEqual(retrieved.nanosecond, 123456000)

    def test_datetime_conversion_nested_dict(self):
        """Test datetime conversion in nested dictionaries."""
        dt1 = datetime(2023, 1, 1, 12, 0, 0)
        dt2 = datetime(2023, 6, 15, 18, 30, 0)

        self.db.collection('test').document('doc1').set({
            'data': {
                'created': dt1,
                'nested': {
                    'updated': dt2
                }
            }
        })

        doc = self.db.collection('test').document('doc1').get()
        data = doc.to_dict()

        self.assertIsInstance(data['data']['created'], DatetimeWithNanoseconds)
        self.assertIsInstance(data['data']['nested']['updated'], DatetimeWithNanoseconds)

    def test_datetime_conversion_in_list(self):
        """Test datetime conversion in lists."""
        dt1 = datetime(2023, 1, 1)
        dt2 = datetime(2023, 6, 15)

        self.db.collection('test').document('doc1').set({
            'timestamps': [dt1, dt2, 'string', 123]
        })

        doc = self.db.collection('test').document('doc1').get()
        timestamps = doc.to_dict()['timestamps']

        self.assertIsInstance(timestamps[0], DatetimeWithNanoseconds)
        self.assertIsInstance(timestamps[1], DatetimeWithNanoseconds)
        self.assertEqual(timestamps[2], 'string')
        self.assertEqual(timestamps[3], 123)

    def test_datetime_with_timezone(self):
        """Test datetime with timezone information."""
        dt_utc = datetime(2023, 11, 28, 12, 0, 0, tzinfo=timezone.utc)

        self.db.collection('test').document('doc1').set({
            'timestamp': dt_utc
        })

        doc = self.db.collection('test').document('doc1').get()
        retrieved = doc.to_dict()['timestamp']

        self.assertIsInstance(retrieved, DatetimeWithNanoseconds)
        self.assertEqual(retrieved.tzinfo, timezone.utc)

    def test_datetime_with_nanoseconds_behaves_like_datetime(self):
        """Test that DatetimeWithNanoseconds can be used like a regular datetime."""
        now = datetime.now()

        self.db.collection('test').document('doc1').set({
            'timestamp': now
        })

        doc = self.db.collection('test').document('doc1').get()
        dt_with_nanos = doc.to_dict()['timestamp']

        # Should be able to use datetime methods
        self.assertTrue(callable(dt_with_nanos.isoformat))
        self.assertTrue(callable(dt_with_nanos.strftime))

        # Should work in comparisons
        later = datetime.now()
        self.assertLess(dt_with_nanos, later)

    def test_multiple_datetime_fields(self):
        """Test document with multiple datetime fields."""
        created = datetime(2023, 1, 1, 10, 0, 0)
        updated = datetime(2023, 11, 28, 15, 30, 0)

        self.db.collection('test').document('doc1').set({
            'name': 'Test',
            'created_at': created,
            'updated_at': updated,
            'count': 42
        })

        doc = self.db.collection('test').document('doc1').get()
        data = doc.to_dict()

        self.assertEqual(data['name'], 'Test')
        self.assertIsInstance(data['created_at'], DatetimeWithNanoseconds)
        self.assertIsInstance(data['updated_at'], DatetimeWithNanoseconds)
        self.assertEqual(data['count'], 42)

    def test_datetime_with_nanoseconds_direct_creation(self):
        """Test creating DatetimeWithNanoseconds directly."""
        # Create with explicit nanosecond
        dt = DatetimeWithNanoseconds(2023, 11, 28, 12, 30, 45, 123456, nanosecond=123456789)

        self.assertEqual(dt.year, 2023)
        self.assertEqual(dt.month, 11)
        self.assertEqual(dt.day, 28)
        self.assertEqual(dt.hour, 12)
        self.assertEqual(dt.minute, 30)
        self.assertEqual(dt.second, 45)
        self.assertEqual(dt.microsecond, 123456)
        self.assertEqual(dt.nanosecond, 123456789)

    def test_datetime_with_nanoseconds_validation(self):
        """Test that nanosecond validation works."""
        # Valid range: 0-999999999
        dt = DatetimeWithNanoseconds(2023, 1, 1, nanosecond=0)
        self.assertEqual(dt.nanosecond, 0)

        dt = DatetimeWithNanoseconds(2023, 1, 1, nanosecond=999999999)
        self.assertEqual(dt.nanosecond, 999999999)

        # Invalid values should raise ValueError
        with self.assertRaises(ValueError):
            DatetimeWithNanoseconds(2023, 1, 1, nanosecond=-1)

        with self.assertRaises(ValueError):
            DatetimeWithNanoseconds(2023, 1, 1, nanosecond=1000000000)

    def test_datetime_with_nanoseconds_type_validation(self):
        """Test that nanosecond type validation works."""
        with self.assertRaises(TypeError):
            DatetimeWithNanoseconds(2023, 1, 1, nanosecond='not an int')

        with self.assertRaises(TypeError):
            DatetimeWithNanoseconds(2023, 1, 1, nanosecond=12.5)

    def test_already_converted_datetime_not_reconverted(self):
        """Test that DatetimeWithNanoseconds objects are not re-converted."""
        dt_with_nanos = DatetimeWithNanoseconds(2023, 11, 28, nanosecond=123456789)

        self.db.collection('test').document('doc1').set({
            'timestamp': dt_with_nanos
        })

        doc = self.db.collection('test').document('doc1').get()
        retrieved = doc.to_dict()['timestamp']

        # Should still be the same type with same nanosecond value
        self.assertIsInstance(retrieved, DatetimeWithNanoseconds)
        self.assertEqual(retrieved.nanosecond, 123456789)

    def test_non_datetime_values_unchanged(self):
        """Test that non-datetime values are not affected."""
        self.db.collection('test').document('doc1').set({
            'string': 'hello',
            'number': 42,
            'boolean': True,
            'none': None,
            'list': [1, 2, 3],
            'dict': {'a': 1, 'b': 2}
        })

        doc = self.db.collection('test').document('doc1').get()
        data = doc.to_dict()

        self.assertEqual(data['string'], 'hello')
        self.assertEqual(data['number'], 42)
        self.assertEqual(data['boolean'], True)
        self.assertIsNone(data['none'])
        self.assertEqual(data['list'], [1, 2, 3])
        self.assertEqual(data['dict'], {'a': 1, 'b': 2})


if __name__ == '__main__':
    unittest.main()
