import unittest

from mockfirestore import MockFirestore


class TestCollectionGroupPathNotation(unittest.TestCase):
    def setUp(self):
        self.mock_db = MockFirestore()
        
        # Create collections using path notation
        self.mock_db.collection('products').document('product1').set({
            'name': 'Product 1',
            'price': 100
        })
        
        # Create nested collections using path notation
        self.mock_db.collection('products/product1/reviews').document('review1').set({
            'user': 'User 1',
            'rating': 5,
            'comment': 'Great product!'
        })
        
        self.mock_db.collection('products/product2/reviews').document('review2').set({
            'user': 'User 2',
            'rating': 4,
            'comment': 'Good product'
        })
        
        # Create a collection with the same name at a different level using path notation
        # First create parent document
        self.mock_db.collection('users').document('user1').set({
            'name': 'Test User'
        })
        # Then create subcollection
        self.mock_db.collection('users').document('user1').collection('reviews').document('review3').set({
            'user': 'User 3',
            'rating': 3,
            'comment': 'Average product'
        })
        
        # Create a deeply nested collection using path notation
        # First create parent collections/documents
        self.mock_db.collection('categories').document('electronics').set({})
        self.mock_db.collection('categories/electronics/products').document('laptop').set({})
        # Then create the nested collection
        self.mock_db.collection('categories/electronics/products/laptop/reviews').document('review4').set({
            'user': 'User 4',
            'rating': 5,
            'comment': 'Excellent laptop!'
        })

    def test_collection_group_with_path_notation(self):
        # Get collection group for 'reviews'
        reviews = self.mock_db.collection_group('reviews')
        results = reviews.get()
        
        # Should find all 4 reviews
        self.assertEqual(len(results), 4)
        
        # Check the ratings to make sure we're getting the right documents
        ratings = [doc.to_dict().get('rating') for doc in results]
        self.assertIn(5, ratings)  # From products/product1/reviews/review1
        self.assertIn(4, ratings)  # From products/product2/reviews/review2
        self.assertIn(3, ratings)  # From users/user1/reviews/review3
        
        # Check that we found the deeply nested review
        comments = [doc.to_dict().get('comment') for doc in results]
        self.assertIn('Great product!', comments)
        self.assertIn('Good product', comments)
        self.assertIn('Average product', comments)
        self.assertIn('Excellent laptop!', comments)  # From categories/electronics/products/laptop/reviews/review4

    def test_collection_group_query_with_path_notation(self):
        # Get collection group for 'reviews' with a query
        reviews = self.mock_db.collection_group('reviews').where('rating', '>=', 4)
        results = reviews.get()
        
        high_ratings = []
        for doc in results:
            data = doc.to_dict()
            if data.get('rating', 0) >= 4:
                high_ratings.append(data.get('rating'))
        
        # Should find the ratings >= 4 (from review1, review2, and review4)
        self.assertGreaterEqual(len(high_ratings), 2)
        self.assertIn(5, high_ratings)  # From review1 and review4
        self.assertIn(4, high_ratings)  # From review2

    def test_collection_group_documents_paths(self):
        # Get collection group for 'reviews'
        reviews = self.mock_db.collection_group('reviews')
        results = reviews.get()
        
        # Verify the documents exist by checking their content
        found_reviews = set()
        for doc in results:
            data = doc.to_dict()
            if data.get('user') == 'User 1' and data.get('rating') == 5:
                found_reviews.add('review1')
            elif data.get('user') == 'User 2' and data.get('rating') == 4:
                found_reviews.add('review2')
            elif data.get('user') == 'User 3' and data.get('rating') == 3:
                found_reviews.add('review3')
            elif data.get('user') == 'User 4' and data.get('comment') == 'Excellent laptop!':
                found_reviews.add('review4')
        
        self.assertIn('review1', found_reviews)
        self.assertIn('review2', found_reviews)
        self.assertIn('review3', found_reviews)
        self.assertIn('review4', found_reviews)

    def test_mixed_collection_creation_methods(self):
        # Create a collection using direct path
        self.mock_db.collection('blogs/blog1/comments').document('comment1').set({
            'user': 'User 5',
            'text': 'Great blog post!'
        })
        
        # Create the same level collection using chained methods
        self.mock_db.collection('blogs').document('blog2').collection('comments').document('comment2').set({
            'user': 'User 6',
            'text': 'Interesting article'
        })
        
        # Get collection group for 'comments'
        comments = self.mock_db.collection_group('comments')
        results = comments.get()
        
        # Should find both comments regardless of creation method
        self.assertEqual(len(results), 2)
        
        texts = [doc.to_dict().get('text') for doc in results]
        self.assertIn('Great blog post!', texts)
        self.assertIn('Interesting article', texts)
        
        # Verify we got the right comments by checking user fields
        users = [doc.to_dict().get('user') for doc in results]
        self.assertIn('User 5', users)
        self.assertIn('User 6', users)

    def test_collection_group_with_empty_collections(self):
        # Create an empty collection with path notation
        empty_collection_ref = self.mock_db.collection('products/product3/reviews')
        
        # Get collection group for 'reviews'
        reviews = self.mock_db.collection_group('reviews')
        results = reviews.get()
        
        # Should still find only the 4 existing reviews (empty collections don't return documents)
        self.assertEqual(len(results), 4)
        
        # Create a document in the previously empty collection
        empty_collection_ref.document('review5').set({
            'user': 'User 7',
            'rating': 2,
            'comment': 'Not great'
        })
        
        # Get collection group again
        reviews = self.mock_db.collection_group('reviews')
        results = reviews.get()
        
        # Now should find 5 reviews
        self.assertEqual(len(results), 5)
