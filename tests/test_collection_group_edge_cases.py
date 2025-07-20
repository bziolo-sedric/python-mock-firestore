import unittest

from mockfirestore import MockFirestore

class TestCollectionGroupEdgeCases(unittest.TestCase):
    def setUp(self):
        self.mock_db = MockFirestore()
        
        # Document with a field named the same as a collection we'll query
        self.mock_db.collection('users').document('user1').set({
            'name': 'Test User 1',
            'settings': {  # This is a field, not a collection
                'theme': 'dark',
                'notifications': True
            }
        })
        
        # Document with nested field having the same structure as collections
        self.mock_db.collection('users').document('user2').set({
            'name': 'Test User 2',
            'settings': {  # This is a field, not a collection
                'doc1': {  # This looks like a document but is just a field
                    'theme': 'light'
                },
                'doc2': {  # This looks like a document but is just a field
                    'notifications': False
                }
            }
        })
        
        # Actual 'settings' collections at different levels
        self.mock_db.collection('settings').document('global').set({
            'maintenance_mode': False
        })
        
        self.mock_db.collection('users').document('user3').collection('settings').document('personal').set({
            'theme': 'system'
        })

    def test_collection_group_ignores_document_fields(self):
        # Get collection group for 'settings'
        settings = self.mock_db.collection_group('settings')
        results = settings.get()
        
        # Should find only 2 settings documents (from the actual collections, not the document fields)
        self.assertEqual(len(results), 2)
        
        # Verify we got the correct documents
        doc_contents = [doc.to_dict() for doc in results]
        self.assertIn({'maintenance_mode': False}, doc_contents)
        self.assertIn({'theme': 'system'}, doc_contents)
        
        # Make sure we didn't get the document fields
        self.assertNotIn({'theme': 'dark', 'notifications': True}, doc_contents)
        
    def test_collection_group_handles_deeply_nested_fields(self):
        # Add document with deeply nested field that looks like collection/document structure
        self.mock_db.collection('products').document('product1').set({
            'name': 'Test Product',
            'variants': {  # This is a field, not a collection
                'small': {  # This looks like a document but is just a field
                    'color': {  # This looks like a collection but is just a field
                        'red': {  # This looks like a document but is just a field
                            'price': 9.99
                        }
                    }
                }
            }
        })
        
        # Create an actual 'color' collection
        self.mock_db.collection('color').document('blue').set({
            'hex': '#0000FF'
        })
        
        # Get collection group for 'color'
        color_group = self.mock_db.collection_group('color')
        results = color_group.get()
        
        # Should find only 1 color document (from the actual collection, not the document field)
        self.assertEqual(len(results), 1)
        
        # Verify we got the correct document
        self.assertEqual(results[0].to_dict(), {'hex': '#0000FF'})

if __name__ == '__main__':
    unittest.main()
