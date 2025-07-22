import unittest
import sys
from typing import Dict, Any

from mockfirestore._helpers import (
    calculate_document_size, 
    collection_mark_path_element,
    FIRESTORE_DOCUMENT_SIZE_LIMIT
)


class TestDocumentSizeCalculation(unittest.TestCase):
    """Test the document size calculation functionality."""

    def test_basic_document_size(self):
        """Test basic document size calculation."""
        doc = {"field1": "value1", "field2": 123}
        size = calculate_document_size(doc)
        self.assertGreater(size, 0)
        self.assertLess(size, 100)  # Should be very small

    def test_nested_document_size(self):
        """Test nested document size calculation."""
        doc = {
            "field1": "value1",
            "nested": {
                "subfield1": "subvalue1",
                "subfield2": 123
            }
        }
        size = calculate_document_size(doc)
        self.assertGreater(size, 0)
        self.assertLess(size, 200)  # Still relatively small

    def test_large_document_size(self):
        """Test large document size calculation."""
        # Create a document that should be close to 1MB
        large_string = "x" * 500000
        doc = {
            "field1": large_string,
            "field2": large_string
        }
        size = calculate_document_size(doc)
        self.assertGreater(size, 900000)  # Should be close to 1MB
        self.assertGreater(FIRESTORE_DOCUMENT_SIZE_LIMIT, size)  # But still under limit

    def test_array_document_size(self):
        """Test document with arrays size calculation."""
        doc = {
            "array1": [1, 2, 3, 4, 5],
            "array2": ["value1", "value2", "value3"]
        }
        size = calculate_document_size(doc)
        self.assertGreater(size, 0)
        self.assertLess(size, 200)  # Small arrays

    def test_collection_markers_excluded(self):
        """Test that collection markers are excluded from size calculation."""
        # Create a document with a collection marker
        collection_marker = collection_mark_path_element("subcollection")
        doc = {
            "field1": "value1",
            collection_marker: {  # This should be excluded from the size calculation
                "doc1": {"field": "value"},
                "doc2": {"field": "value"}
            }
        }
        
        # Calculate size
        size_with_collection = calculate_document_size(doc)
        
        # Create an equivalent document without the collection
        doc_without_collection = {
            "field1": "value1"
        }
        size_without_collection = calculate_document_size(doc_without_collection)
        
        # The sizes should be very close, as the collection marker should be excluded
        # We allow a small difference due to potential overhead
        self.assertAlmostEqual(size_with_collection, size_without_collection, delta=20)

    def test_different_data_types(self):
        """Test document size calculation with different data types."""
        doc = {
            "null_field": None,
            "bool_field": True,
            "int_field": 12345,
            "float_field": 123.45,
            "string_field": "hello world",
            "array_field": [1, 2, 3, "string", True, None],
            "nested_field": {
                "sub_field": "value",
                "sub_array": [4, 5, 6]
            }
        }
        size = calculate_document_size(doc)
        self.assertGreater(size, 0)
        # Hard to predict exact size, but should be reasonable
        self.assertLess(size, 500)


if __name__ == '__main__':
    unittest.main()
