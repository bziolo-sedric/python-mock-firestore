"""Tests for DatetimeWithNanoseconds functionality in async operations."""
import unittest
from datetime import datetime, timezone
from mockfirestore import AsyncMockFirestore, DatetimeWithNanoseconds


class TestAsyncDatetimeWithNanoseconds(unittest.IsolatedAsyncioTestCase):
    """Test DatetimeWithNanoseconds class and conversion in async mode."""

    def setUp(self):
        """Set up test fixtures."""
        self.db = AsyncMockFirestore()

    async def test_async_datetime_conversion_simple(self):
        """Test that regular datetime is converted to DatetimeWithNanoseconds in async."""
        now = datetime.now()

        # Store a document with a datetime
        await self.db.collection('test').document('doc1').set({
            'created_at': now
        })

        # Retrieve and check type
        doc = await self.db.collection('test').document('doc1').get()
        retrieved_datetime = doc.to_dict()['created_at']

        self.assertIsInstance(retrieved_datetime, DatetimeWithNanoseconds)
        self.assertTrue(hasattr(retrieved_datetime, 'nanosecond'))
        self.assertIsInstance(retrieved_datetime.nanosecond, int)
        self.assertGreaterEqual(retrieved_datetime.nanosecond, 0)
        self.assertLessEqual(retrieved_datetime.nanosecond, 999999999)

    async def test_async_datetime_conversion_preserves_values(self):
        """Test that datetime values are preserved during conversion in async."""
        original_datetime = datetime(2023, 11, 28, 14, 30, 45, 123456)

        await self.db.collection('test').document('doc1').set({
            'timestamp': original_datetime
        })

        doc = await self.db.collection('test').document('doc1').get()
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

    async def test_async_datetime_conversion_nested_dict(self):
        """Test datetime conversion in nested dictionaries in async."""
        dt1 = datetime(2023, 1, 1, 12, 0, 0)
        dt2 = datetime(2023, 6, 15, 18, 30, 0)

        await self.db.collection('test').document('doc1').set({
            'data': {
                'created': dt1,
                'nested': {
                    'updated': dt2
                }
            }
        })

        doc = await self.db.collection('test').document('doc1').get()
        data = doc.to_dict()

        self.assertIsInstance(data['data']['created'], DatetimeWithNanoseconds)
        self.assertIsInstance(data['data']['nested']['updated'], DatetimeWithNanoseconds)

    async def test_async_datetime_conversion_in_list(self):
        """Test datetime conversion in lists in async."""
        dt1 = datetime(2023, 1, 1)
        dt2 = datetime(2023, 6, 15)

        await self.db.collection('test').document('doc1').set({
            'timestamps': [dt1, dt2, 'string', 123]
        })

        doc = await self.db.collection('test').document('doc1').get()
        timestamps = doc.to_dict()['timestamps']

        self.assertIsInstance(timestamps[0], DatetimeWithNanoseconds)
        self.assertIsInstance(timestamps[1], DatetimeWithNanoseconds)
        self.assertEqual(timestamps[2], 'string')
        self.assertEqual(timestamps[3], 123)

    async def test_async_datetime_with_timezone(self):
        """Test datetime with timezone information in async."""
        dt_utc = datetime(2023, 11, 28, 12, 0, 0, tzinfo=timezone.utc)

        await self.db.collection('test').document('doc1').set({
            'timestamp': dt_utc
        })

        doc = await self.db.collection('test').document('doc1').get()
        retrieved = doc.to_dict()['timestamp']

        self.assertIsInstance(retrieved, DatetimeWithNanoseconds)
        self.assertEqual(retrieved.tzinfo, timezone.utc)

    async def test_async_multiple_datetime_fields(self):
        """Test document with multiple datetime fields in async."""
        created = datetime(2023, 1, 1, 10, 0, 0)
        updated = datetime(2023, 11, 28, 15, 30, 0)

        await self.db.collection('test').document('doc1').set({
            'name': 'Test',
            'created_at': created,
            'updated_at': updated,
            'count': 42
        })

        doc = await self.db.collection('test').document('doc1').get()
        data = doc.to_dict()

        self.assertEqual(data['name'], 'Test')
        self.assertIsInstance(data['created_at'], DatetimeWithNanoseconds)
        self.assertIsInstance(data['updated_at'], DatetimeWithNanoseconds)
        self.assertEqual(data['count'], 42)


if __name__ == '__main__':
    unittest.main()
