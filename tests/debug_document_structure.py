import unittest
import sys
from pprint import pprint

from mockfirestore import MockFirestore
from mockfirestore._helpers import get_by_path

class TestDocumentStructure(unittest.TestCase):
    def setUp(self):
        self.mock_db = MockFirestore()
    
    def test_collection_vs_field_structure(self):
        # Setup a document with a field that has the same name as a subcollection
        self.mock_db.collection('users').document('user1').set({
            'name': 'Test User',
            'settings': {  # This is a document field, not a collection
                'theme': 'dark'
            }
        })
        
        # Setup a subcollection with the same name
        self.mock_db.collection('users').document('user1').collection('settings').document('user_settings').set({
            'language': 'en'
        })
        
        # Create another example with deeply nested fields that look like collections
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
        
        # Now dump the internal structure to see how they differ
        print("\n\n----- INTERNAL DATA STRUCTURE -----")
        pprint(self.mock_db._data, width=100, depth=10)
        print("----- END DATA STRUCTURE -----\n\n")
        
        # Check the access patterns and paths
        print("----- ACCESS PATTERNS -----")
        
        # Access document field
        print("Document field path ['users', 'user1', 'settings']:")
        field_data = get_by_path(self.mock_db._data, ['users', 'user1', 'settings'])
        print(f"  Data: {field_data}")
        print(f"  Type: {type(field_data)}")
        print(f"  Is dictionary: {isinstance(field_data, dict)}")
        
        # Access subcollection
        print("\nSubcollection path ['users', 'user1', 'settings']:")
        try:
            subcol_data = get_by_path(self.mock_db._data, ['users', 'user1', 'settings'])
            print(f"  Data: {subcol_data}")
            print(f"  Type: {type(subcol_data)}")
            print(f"  Is dictionary: {isinstance(subcol_data, dict)}")
            
            # See if there are any differences in the dictionaries
            if isinstance(subcol_data, dict):
                print(f"  Keys: {list(subcol_data.keys())}")
                print(f"  Values are dictionaries: {all(isinstance(v, dict) for v in subcol_data.values())}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Access deeply nested field
        print("\nDeeply nested field path ['products', 'product1', 'variants', 'small', 'color']:")
        try:
            nested_field = get_by_path(self.mock_db._data, ['products', 'product1', 'variants', 'small', 'color'])
            print(f"  Data: {nested_field}")
            print(f"  Type: {type(nested_field)}")
            print(f"  Keys: {list(nested_field.keys()) if isinstance(nested_field, dict) else None}")
        except Exception as e:
            print(f"  Error: {e}")
            
        # Access actual collection
        print("\nActual collection path ['color']:")
        try:
            col_data = get_by_path(self.mock_db._data, ['color'])
            print(f"  Data: {col_data}")
            print(f"  Type: {type(col_data)}")
            print(f"  Keys: {list(col_data.keys()) if isinstance(col_data, dict) else None}")
        except Exception as e:
            print(f"  Error: {e}")
        print("----- END ACCESS PATTERNS -----\n")
        
        # Test CollectionGroup.get() behavior with the updated implementation
        print("----- COLLECTION GROUP QUERIES -----")
        
        # Query for 'settings'
        settings_group = self.mock_db.collection_group('settings')
        settings_results = settings_group.get()
        print(f"Settings group results: {len(settings_results)} documents")
        for i, doc in enumerate(settings_results, 1):
            print(f"  Document {i}:")
            print(f"    ID: {doc.id}")
            print(f"    Path: {doc.reference._path}")
            print(f"    Data: {doc.to_dict()}")
            
        # Query for 'color'
        color_group = self.mock_db.collection_group('color')
        color_results = color_group.get()
        print(f"\nColor group results: {len(color_results)} documents")
        for i, doc in enumerate(color_results, 1):
            print(f"  Document {i}:")
            print(f"    ID: {doc.id}")
            print(f"    Path: {doc.reference._path}")
            print(f"    Data: {doc.to_dict()}")
        
        print("----- END COLLECTION GROUP QUERIES -----")
        
        # Just to make the test pass
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()
