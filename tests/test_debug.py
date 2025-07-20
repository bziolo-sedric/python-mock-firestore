import unittest

from mockfirestore import MockFirestore

class TestDebug(unittest.TestCase):
    def setUp(self):
        self.mock_db = MockFirestore()
        
        # Document with a field named the same as a collection
        self.mock_db.collection('users').document('user1').set({
            'name': 'Test User 1',
            'settings': {  # This is a field, not a collection
                'theme': 'dark',
                'notifications': True
            }
        })
        
        # Actual 'settings' collections at different levels
        self.mock_db.collection('settings').document('global').set({
            'maintenance_mode': False
        })
        
        self.mock_db.collection('users').document('user3').collection('settings').document('personal').set({
            'theme': 'system'
        })
        
        # Print the tracked collection paths
        print("\n_collection_paths tracked by MockFirestore:")
        for collection_ref in self.mock_db.collections():
            print(f"  {collection_ref._path}")
        
        # Debug collection group query
        settings = self.mock_db.collection_group('settings')
        results = settings.get()
        
        print("\nCollection group query for 'settings' found:")
        print(f"  {len(results)} documents")
        for doc in results:
            print(f"  - Path: {doc.reference._path}, Data: {doc.to_dict()}")

    def test_debug(self):
        # Just make the test pass
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
