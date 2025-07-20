import asyncio
import unittest
from mockfirestore import AsyncMockFirestore


class TestAsyncAggregationQuery(unittest.TestCase):
    def setUp(self):
        self.db = AsyncMockFirestore()
        # Set up test data using asyncio
        asyncio.run(self.setup_data())

    async def setup_data(self):
        # Add some test data
        await self.db.collection('users').document('user1').set({
            'name': 'Alice',
            'age': 30,
            'salary': 50000
        })
        await self.db.collection('users').document('user2').set({
            'name': 'Bob',
            'age': 25,
            'salary': 60000
        })
        await self.db.collection('users').document('user3').set({
            'name': 'Charlie',
            'age': 35,
            'salary': 70000
        })

    def test_count_query(self):
        """Test counting documents in a collection."""
        # Count all users
        count_result = asyncio.run(self.db.collection('users').count().get())
        self.assertEqual(count_result['field_0'], 3)
        
        # Count with custom alias
        count_result = asyncio.run(self.db.collection('users').count(alias='total').get())
        self.assertEqual(count_result['total'], 3)
        
        # Count with filter
        count_result = asyncio.run(self.db.collection('users').where('age', '>', 30).count().get())
        self.assertEqual(count_result['field_0'], 1)

    def test_sum_query(self):
        """Test summing values in a collection."""
        # Sum all salaries
        sum_result = asyncio.run(self.db.collection('users').sum('salary').get())
        self.assertEqual(sum_result['field_0'], 180000)
        
        # Sum with custom alias
        sum_result = asyncio.run(self.db.collection('users').sum('salary', alias='total_salary').get())
        self.assertEqual(sum_result['total_salary'], 180000)
        
        # Sum with filter
        sum_result = asyncio.run(self.db.collection('users').where('age', '<', 30).sum('salary').get())
        self.assertEqual(sum_result['field_0'], 60000)

    def test_avg_query(self):
        """Test averaging values in a collection."""
        # Average all ages
        avg_result = asyncio.run(self.db.collection('users').avg('age').get())
        self.assertEqual(avg_result['field_0'], 30)
        
        # Average with custom alias
        avg_result = asyncio.run(self.db.collection('users').avg('age', alias='average_age').get())
        self.assertEqual(avg_result['average_age'], 30)
        
        # Average with filter
        avg_result = asyncio.run(self.db.collection('users').where('salary', '>=', 60000).avg('age').get())
        self.assertEqual(avg_result['field_0'], 30)

    def test_multiple_aggregations(self):
        """Test running multiple aggregations in one query."""
        # Count, sum and average in a single query
        query = self.db.collection('users')
        agg_query = query.count(alias='user_count').sum('salary', alias='total_salary').avg('age', alias='average_age')
        result = asyncio.run(agg_query.get())
        
        self.assertEqual(result['user_count'], 3)
        self.assertEqual(result['total_salary'], 180000)
        self.assertEqual(result['average_age'], 30)

    @unittest.skip("Nested collection group queries not supported correctly in this version")
    def test_collection_group_count(self):
        """Test counting documents in a collection group."""
        async def setup_collection_group():
            # Set up nested collections
            await self.db.collection('cities').document('SF').collection('landmarks').document('bridge').set({
                'name': 'Golden Gate Bridge'
            })
            await self.db.collection('cities').document('NYC').collection('landmarks').document('statue').set({
                'name': 'Statue of Liberty'
            })
            await self.db.collection('cities').document('DC').collection('landmarks').document('monument').set({
                'name': 'Washington Monument'
            })
            
            # Count landmarks
            result = await self.db.collection_group('landmarks').count().get()
            return result
            
        result = asyncio.run(setup_collection_group())
        self.assertEqual(result['field_0'], 3)

    def test_query_count(self):
        """Test counting filtered documents with a query."""
        async def test():
            query = self.db.collection('users').where('age', '>', 25)
            result = await query.count(alias='older_users').get()
            return result
            
        result = asyncio.run(test())
        self.assertEqual(result['older_users'], 2)

    def test_aggregation_result_methods(self):
        """Test aggregation result access methods."""
        async def test():
            result = await self.db.collection('users').count(alias='count').get()
            return result
            
        result = asyncio.run(test())
        
        # Test dictionary-like access
        self.assertEqual(result['count'], 3)
        
        # Test contains check
        self.assertTrue('count' in result)
        self.assertFalse('nonexistent' in result)
        
        # Test to_dict method
        dict_result = result.to_dict()
        self.assertEqual(dict_result, {'count': 3})


if __name__ == '__main__':
    unittest.main()
