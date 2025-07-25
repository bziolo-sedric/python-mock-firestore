import unittest

from mockfirestore import MockFirestore


class TestCollectionGroupActualCollectionInclusion(unittest.TestCase):
    """
    Tests specifically focused on ensuring actual collections with the same name
    are properly included in collection group queries.
    """
    
    def setUp(self):
        self.mock_db = MockFirestore()
        
        # Create a top-level collection with the name we'll use for collection_group
        self.mock_db.collection('reviews').document('top_level_review').set({
            'user': 'Top Level User',
            'rating': 5,
            'text': 'This is a top-level review'
        })
        
        # Create a nested collection with the same name
        self.mock_db.collection('products').document('product1').collection('reviews').document('nested_review').set({
            'user': 'Nested User',
            'rating': 4,
            'text': 'This is a nested review'
        })

    def test_collection_group_includes_top_level_collection(self):
        """
        Test to verify that when using collection_group, documents from a top-level collection
        with the same name are included in the results.
        """
        # Get collection group for 'reviews'
        reviews = self.mock_db.collection_group('reviews')
        results = reviews.get()
        
        # Should find both the top-level and nested reviews
        self.assertEqual(len(results), 2)
        
        # Verify we have both the top-level and nested documents by checking their content
        found_top_level = False
        found_nested = False
        
        for doc in results:
            data = doc.to_dict()
            if data.get('text') == 'This is a top-level review':
                found_top_level = True
            elif data.get('text') == 'This is a nested review':
                found_nested = True
        
        self.assertTrue(found_top_level, "Top-level review should be included in collection group results")
        self.assertTrue(found_nested, "Nested review should be included in collection group results")
        
    def test_collection_group_query_includes_top_level_collection(self):
        """
        Test to verify that when using collection_group with a query,
        documents from a top-level collection that match the query are included.
        """
        # Get collection group for 'reviews' with a query
        reviews = self.mock_db.collection_group('reviews').where('rating', '==', 5)
        results = reviews.get()
        
        # Should find only the top-level review with rating 5
        self.assertEqual(len(results), 1)
        
        # Verify it's the top-level document
        self.assertEqual(results[0].to_dict().get('text'), 'This is a top-level review')
        
        # Try another query that should match the nested document
        reviews = self.mock_db.collection_group('reviews').where('rating', '==', 4)
        results = reviews.get()
        
        # Should find only the nested review with rating 4
        self.assertEqual(len(results), 1)
        
        # Verify it's the nested document
        self.assertEqual(results[0].to_dict().get('text'), 'This is a nested review')
        
    def test_collection_group_with_many_collections(self):
        """
        Test to verify that collection_group finds all collections with the specified name,
        regardless of nesting level or position in the document hierarchy.
        """
        # Add more collections with the same name at different levels
        self.mock_db.collection('categories').document('category1').collection('reviews').document('category_review').set({
            'user': 'Category User',
            'rating': 3,
            'text': 'This is a category review'
        })
        
        self.mock_db.collection('users').document('user1').collection('posts').document('post1').collection('reviews').document('deeply_nested_review').set({
            'user': 'Deep Nest User',
            'rating': 2,
            'text': 'This is a deeply nested review'
        })
        
        # Get collection group for 'reviews'
        reviews = self.mock_db.collection_group('reviews')
        results = reviews.get()
        
        # Should find all 4 reviews
        self.assertEqual(len(results), 4)
        
        # Verify we have documents from all collection levels
        texts = set(doc.to_dict().get('text') for doc in results)
        expected_texts = {
            'This is a top-level review',
            'This is a nested review',
            'This is a category review',
            'This is a deeply nested review'
        }
        
        self.assertEqual(texts, expected_texts)
        
    def test_collection_reference_vs_collection_group(self):
        """
        Test to compare the behavior of a regular collection reference vs a collection group
        when there are collections with the same name at different levels.
        """
        # Get a direct reference to the top-level 'reviews' collection
        top_level_reviews = self.mock_db.collection('reviews')
        top_level_results = top_level_reviews.get()
        
        # Should find only 1 document in the top-level collection
        self.assertEqual(len(top_level_results), 1)
        self.assertEqual(top_level_results[0].to_dict().get('text'), 'This is a top-level review')
        
        # Get collection group for 'reviews'
        reviews_group = self.mock_db.collection_group('reviews')
        group_results = reviews_group.get()
        
        # Should find both documents
        self.assertEqual(len(group_results), 2)
        
        # Verify the collection group returns both the top-level and nested documents
        texts = set(doc.to_dict().get('text') for doc in group_results)
        expected_texts = {'This is a top-level review', 'This is a nested review'}
        
        self.assertEqual(texts, expected_texts)


if __name__ == '__main__':
    unittest.main()
