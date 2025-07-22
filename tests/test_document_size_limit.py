import unittest
import sys
from unittest import TestCase
import json

from mockfirestore import MockFirestore, InvalidArgument
from mockfirestore._helpers import FIRESTORE_DOCUMENT_SIZE_LIMIT, collection_mark_path_element
from mockfirestore._transformations import preview_transformations


class TestDocumentSizeLimit(TestCase):
    def setUp(self):
        self.fs = MockFirestore()
        
    def test_document_create_exceeds_size_limit(self):
        """Test that creating a document that exceeds the size limit raises an exception."""
        # Generate a large document that exceeds 1MB
        large_data = {"large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT + 100)}
        
        # Verify the document size calculation works
        json_str = json.dumps(large_data)
        self.assertGreater(sys.getsizeof(json_str), FIRESTORE_DOCUMENT_SIZE_LIMIT)
        
        # Test create method
        with self.assertRaises(InvalidArgument) as context:
            self.fs.collection('test').document('doc1').create(large_data)
        
        self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
    
    def test_document_set_exceeds_size_limit(self):
        """Test that setting a document that exceeds the size limit raises an exception."""
        # Generate a large document that exceeds 1MB
        large_data = {"large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT + 100)}
        
        # Test set method
        with self.assertRaises(InvalidArgument) as context:
            self.fs.collection('test').document('doc1').set(large_data)
        
        self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
    
    def test_document_update_exceeds_size_limit(self):
        """Test that updating a document that would exceed the size limit raises an exception."""
        # Create a document that's close to the limit
        initial_data = {"field1": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT - 5000)}
        doc_ref = self.fs.collection('test').document('doc1')
        doc_ref.set(initial_data)
        
        # Try to update it with data that would push it over the limit
        update_data = {"field2": "y" * 10000}
        
        with self.assertRaises(InvalidArgument) as context:
            doc_ref.update(update_data)
        
        self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
    
    def test_transaction_create_exceeds_size_limit(self):
        """Test that creating a document in a transaction that exceeds the size limit raises an exception."""
        # Generate a large document that exceeds 1MB
        large_data = {"large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT + 100)}
        doc_ref = self.fs.collection('test').document('doc1')
        
        # Test create method in a transaction
        transaction = self.fs.transaction()
        transaction._begin()
        
        # Test directly without expecting exception to be caught
        # We just want to verify the code runs correctly and calls the size check
        # We expect the test to pass even though an exception is raised
        # The exception is part of the normal operation of the code
        size_error_raised = False
        try:
            transaction.create(doc_ref, large_data)
        except InvalidArgument as e:
            size_error_raised = True
            self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(e))
        
        self.assertTrue(size_error_raised, "Document size check was not performed")
    
    def test_transaction_set_exceeds_size_limit(self):
        """Test that setting a document in a transaction that exceeds the size limit raises an exception."""
        # Generate a large document that exceeds 1MB
        large_data = {"large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT + 100)}
        doc_ref = self.fs.collection('test').document('doc1')
        
        # Test set method in a transaction
        transaction = self.fs.transaction()
        transaction._begin()
        
        # Test directly without expecting exception to be caught
        # We just want to verify the code runs correctly and calls the size check
        size_error_raised = False
        try:
            transaction.set(doc_ref, large_data)
        except InvalidArgument as e:
            size_error_raised = True
            self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(e))
        
        self.assertTrue(size_error_raised, "Document size check was not performed")
        
    def test_update_with_transformations_doesnt_create_unwanted_documents(self):
        """Test that transformation previews during size checks don't modify the database."""
        # Create a document
        doc_ref = self.fs.collection('test').document('doc1')
        initial_data = {"counter": 10, "array": [1, 2, 3]}
        doc_ref.set(initial_data)
        
        # Create a transformation that would make the document exceed the size limit
        update_data = {
            "increment.counter": 5,  # Should increment counter to 15
            "arrayUnion.array": [4, 5],  # Should add 4 and 5 to the array
            "large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT)  # This will make it exceed the limit
        }
        
        # Update should fail due to size limit
        with self.assertRaises(InvalidArgument) as context:
            doc_ref.update(update_data)
        
        self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
        
        # Verify the document wasn't modified by checking counter and array values
        doc = doc_ref.get().to_dict()
        self.assertEqual(10, doc["counter"], "Counter should remain unchanged after failed update")
        self.assertEqual([1, 2, 3], doc["array"], "Array should remain unchanged after failed update")
        self.assertNotIn("large_field", doc, "Large field shouldn't be added after failed update")
        
    def test_transaction_update_with_transformations_doesnt_create_unwanted_documents(self):
        """Test that transformation previews during transaction size checks don't modify the database."""
        # Create a document
        doc_ref = self.fs.collection('test').document('doc1')
        initial_data = {"counter": 10, "array": [1, 2, 3]}
        doc_ref.set(initial_data)
        
        # Create a transaction with a transformation that would make the document exceed the size limit
        transaction = self.fs.transaction()
        update_data = {
            "increment.counter": 5,  # Should increment counter to 15
            "arrayUnion.array": [4, 5],  # Should add 4 and 5 to the array
            "large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT)  # This will make it exceed the limit
        }
        
        # The transaction itself doesn't throw an exception when update is called,
        # but it should fail when it's committed due to size limit
        transaction._begin()
        
        # This call should succeed since it just schedules the update
        transaction.update(doc_ref, update_data)
        
        # The commit should fail due to size limit
        with self.assertRaises(InvalidArgument) as context:
            transaction._commit()
        
        self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
        
        # Verify the document wasn't modified by checking counter and array values
        doc = doc_ref.get().to_dict()
        self.assertEqual(10, doc["counter"], "Counter should remain unchanged after failed transaction update")
        self.assertEqual([1, 2, 3], doc["array"], "Array should remain unchanged after failed transaction update")
        self.assertNotIn("large_field", doc, "Large field shouldn't be added after failed transaction update")


if __name__ == '__main__':
    unittest.main()
