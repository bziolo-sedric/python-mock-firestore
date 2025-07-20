import unittest

from mockfirestore import MockFirestore


class TestCollectionGroup(unittest.TestCase):
    def setUp(self):
        self.mock_db = MockFirestore()
        
        # Set up test data with a document field that has the same name as a collection
        self.mock_db.collection('users').document('user1').set({
            'name': 'Test User',
            'books': {  # This is a field named 'books', not a collection
                'book1': 'title1',
                'book2': 'title2'
            }
        })
        
        # Create actual collections with the same name at different levels
        self.mock_db.collection('books').document('book1').set({
            'title': 'Book Title 1'
        })
        
        # Subcollection with same name
        self.mock_db.collection('users').document('user2').collection('books').document('book3').set({
            'title': 'Book Title 3'
        })

    def test_collection_group_only_finds_collections(self):
        # Get collection group for 'books'
        books = self.mock_db.collection_group('books')
        results = books.get()
        
        # Should find only 2 books (from the actual collections, not the document field)
        self.assertEqual(len(results), 2)
        
        # Check the titles to make sure we're getting the right documents
        book_titles = [doc.to_dict().get('title') for doc in results]
        self.assertIn('Book Title 1', book_titles)
        self.assertIn('Book Title 3', book_titles)
        
        # Make sure we didn't get document fields
        for doc in results:
            self.assertNotEqual(doc.to_dict(), {'book1': 'title1', 'book2': 'title2'})

    def test_collection_group_handles_nested_collections(self):
        # Add a deeply nested collection
        self.mock_db.collection('level1').document('doc1').collection('level2').document('doc2').collection('books').document('deepbook').set({
            'title': 'Deep Nested Book'
        })
        
        # Get collection group
        books = self.mock_db.collection_group('books')
        results = books.get()
        
        # Should find all 3 books now
        self.assertEqual(len(results), 3)
        
        # Check we found the deep nested book
        found_deep_book = False
        for doc in results:
            if doc.to_dict().get('title') == 'Deep Nested Book':
                found_deep_book = True
                break
        
        self.assertTrue(found_deep_book, "Did not find the deeply nested book")

    def test_collection_group_with_nonexistent_collection(self):
        # Get collection group for a collection that doesn't exist
        nonexistent = self.mock_db.collection_group('nonexistent')
        results = nonexistent.get()
        
        # Should return empty list
        self.assertEqual(len(results), 0)


if __name__ == '__main__':
    unittest.main()
