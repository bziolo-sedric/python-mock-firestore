"""
Based on official Firebase documentation:
https://firebase.google.com/docs/firestore/query-data/aggregation-queries

Real Firestore API behavior:
- results = aggregate_query.get() returns an iterable
- for result in results: result[0].alias gets the alias
- for result in results: result[0].value gets the value
"""
import unittest
from mockfirestore import MockFirestore


class TestRealFirestoreAPICompatibility(unittest.TestCase):
    """Test that AggregationResult matches the real Firestore API."""

    def setUp(self):
        self.db = MockFirestore()
        # Add test data matching Firebase documentation example
        self.db.collection('users').document('user1').set({
            'name': 'Alice',
            'born': 1850,
            'age': 30,
            'salary': 50000
        })
        self.db.collection('users').document('user2').set({
            'name': 'Bob',
            'born': 1820,
            'age': 25,
            'salary': 60000
        })
        self.db.collection('users').document('user3').set({
            'name': 'Charlie',
            'born': 1900,
            'age': 35,
            'salary': 70000
        })

    def test_count_aggregation_real_api_pattern(self):
        """Test count aggregation using the exact pattern from Firebase docs.

        Real Firestore API pattern:
            results = aggregate_query.get()
            for result in results:
                print(f"Alias: {result[0].alias}")
                print(f"Value: {result[0].value}")
        """
        # Create query and aggregation matching Firebase docs
        query = self.db.collection('users').where('born', '>', 1800)
        aggregate_query = query.count(alias='all')

        # Get results
        results = aggregate_query.get()

        # Test the exact API pattern from Firebase documentation
        result_count = 0
        for result in results:
            # This is the exact pattern from Firebase docs
            alias = result[0].alias
            value = result[0].value

            result_count += 1
            self.assertEqual(alias, 'all')
            self.assertEqual(value, 3)  # All 3 users born after 1800

        # Should iterate exactly once for single aggregation
        self.assertEqual(result_count, 1)

    def test_multiple_aggregations_real_api(self):
        """Test multiple aggregations with real Firestore API pattern."""
        # Create multiple aggregations
        query = self.db.collection('users')
        aggregate_query = query.count(alias='total_users').sum('salary', alias='total_salary').avg('age', alias='avg_age')

        results = aggregate_query.get()

        # Collect all results using the real API pattern
        aggregation_data = {}
        for result in results:
            alias = result[0].alias
            value = result[0].value
            aggregation_data[alias] = value

        # Verify all aggregations
        self.assertEqual(aggregation_data['total_users'], 3)
        self.assertEqual(aggregation_data['total_salary'], 180000)
        self.assertEqual(aggregation_data['avg_age'], 30)

    def test_sum_aggregation_real_api(self):
        """Test sum aggregation with real API pattern."""
        query = self.db.collection('users')
        aggregate_query = query.sum('salary', alias='total_salary')

        results = aggregate_query.get()

        for result in results:
            self.assertEqual(result[0].alias, 'total_salary')
            self.assertEqual(result[0].value, 180000)

    def test_avg_aggregation_real_api(self):
        """Test average aggregation with real API pattern."""
        query = self.db.collection('users')
        aggregate_query = query.avg('age', alias='average_age')

        results = aggregate_query.get()

        for result in results:
            self.assertEqual(result[0].alias, 'average_age')
            self.assertEqual(result[0].value, 30)

    def test_result_item_attributes(self):
        """Test that result items have the required .alias and .value attributes."""
        query = self.db.collection('users')
        aggregate_query = query.count(alias='test_count')

        results = aggregate_query.get()

        for result in results:
            item = result[0]

            # Verify attributes exist (as per Firebase API)
            self.assertTrue(hasattr(item, 'alias'))
            self.assertTrue(hasattr(item, 'value'))

            # Verify types and values
            self.assertIsInstance(item.alias, str)
            self.assertEqual(item.alias, 'test_count')
            self.assertEqual(item.value, 3)

    def test_backward_compatibility(self):
        """Test that the old dictionary-style access still works."""
        query = self.db.collection('users')
        aggregate_query = query.count(alias='user_count')

        results = aggregate_query.get()

        # Old style should still work
        self.assertEqual(results['user_count'], 3)
        self.assertTrue('user_count' in results)
        self.assertEqual(results.to_dict(), {'user_count': 3})


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
