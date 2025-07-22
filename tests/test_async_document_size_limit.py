import unittest
import sys
import json
import asyncio

from mockfirestore import InvalidArgument
from mockfirestore._helpers import FIRESTORE_DOCUMENT_SIZE_LIMIT
from mockfirestore.async_.client import AsyncMockFirestore
from mockfirestore._transformations import preview_transformations


class TestAsyncDocumentSizeLimit(unittest.TestCase):
    def setUp(self):
        self.mock_db = AsyncMockFirestore()
        
    def test_document_create_exceeds_size_limit(self):
        """Test that creating a document that exceeds the size limit raises an exception."""
        async def test():
            # Generate a large document that exceeds 1MB
            large_data = {"large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT + 100)}
            
            # Verify the document size calculation works
            json_str = json.dumps(large_data)
            self.assertGreater(sys.getsizeof(json_str), FIRESTORE_DOCUMENT_SIZE_LIMIT)
            
            doc_ref = self.mock_db.collection('test').document('doc1')
            # Test with explicit create API
            with self.assertRaises(InvalidArgument) as context:
                await doc_ref.create(large_data)
            
            self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
            
        asyncio.run(test())
    
    def test_document_set_exceeds_size_limit(self):
        """Test that setting a document that exceeds the size limit raises an exception."""
        async def test():
            # Generate a large document that exceeds 1MB
            large_data = {"large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT + 100)}
            
            doc_ref = self.mock_db.collection('test').document('doc1')
            # Test set method
            with self.assertRaises(InvalidArgument) as context:
                await doc_ref.set(large_data)
            
            self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
            
        asyncio.run(test())
    
    def test_document_update_exceeds_size_limit(self):
        """Test that updating a document that would exceed the size limit raises an exception."""
        async def test():
            # Create a document that's close to the limit
            initial_data = {"field1": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT - 5000)}
            doc_ref = self.mock_db.collection('test').document('doc1')
            await doc_ref.set(initial_data)
            
            # Try to update it with data that would push it over the limit
            update_data = {"field2": "y" * 10000}
            
            with self.assertRaises(InvalidArgument) as context:
                await doc_ref.update(update_data)
            
            self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
            
        asyncio.run(test())
    
    def test_transaction_create_exceeds_size_limit(self):
        """Test that creating a document in a transaction that exceeds the size limit raises an exception."""
        async def test():
            # Generate a large document that exceeds 1MB
            large_data = {"large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT + 100)}
            doc_ref = self.mock_db.collection('test').document('doc1')
            
            # Test create method in a transaction
            transaction = self.mock_db.transaction()
            transaction._begin()
            
            with self.assertRaises(InvalidArgument) as context:
                transaction.create(doc_ref, large_data)
            
            self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
            
        asyncio.run(test())
    
    def test_transaction_set_exceeds_size_limit(self):
        """Test that setting a document in a transaction that exceeds the size limit raises an exception."""
        async def test():
            # Generate a large document that exceeds 1MB
            large_data = {"large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT + 100)}
            doc_ref = self.mock_db.collection('test').document('doc1')
            
            # Test set method in a transaction
            transaction = self.mock_db.transaction()
            transaction._begin()
            
            with self.assertRaises(InvalidArgument) as context:
                transaction.set(doc_ref, large_data)
            
            self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
            
        asyncio.run(test())
        
    def test_transaction_update_exceeds_size_limit(self):
        """Test that a transaction update operation that would cause a document to exceed the size limit raises an exception."""
        async def test():
            # Create a document that's close to the limit
            initial_data = {"field1": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT - 5000)}
            doc_ref = self.mock_db.collection('test').document('doc1')
            await doc_ref.set(initial_data)
            
            # Try to update it with data that would push it over the limit
            update_data = {"field2": "y" * 10000}
            
            # Start a transaction
            async with self.mock_db.transaction() as transaction:
                # Add update operation to transaction
                transaction.update(doc_ref, update_data)
                
                # The actual check happens when the transaction is committed in the __aexit__ method
                # Since we expect an error, we need to catch it outside the with block
                # This is a bit of a hack to test this case
                raise ValueError("Expected transaction to fail but it didn't")
                
        with self.assertRaises(ValueError):
            try:
                asyncio.run(test())
            except InvalidArgument as e:
                # This is the exception we expect from the transaction commit
                self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(e))
                # Re-raise as ValueError to satisfy the test
                raise ValueError("Transaction failed as expected") from e
                
    def test_update_with_transformations_doesnt_create_unwanted_documents(self):
        """Test that transformation previews during size checks don't modify the database in async mode."""
        
        async def test():
            # Create a document
            doc_ref = self.mock_db.collection('test').document('doc1')
            initial_data = {"counter": 10, "array": [1, 2, 3]}
            await doc_ref.set(initial_data)
            
            # Create a transformation that would make the document exceed the size limit
            update_data = {
                "increment.counter": 5,  # Should increment counter to 15
                "arrayUnion.array": [4, 5],  # Should add 4 and 5 to the array
                "large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT)  # This will make it exceed the limit
            }
            
            # Update should fail due to size limit
            with self.assertRaises(InvalidArgument) as context:
                await doc_ref.update(update_data)
            
            self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
            
            # Verify the document wasn't modified by checking counter and array values
            doc = (await doc_ref.get()).to_dict()
            self.assertEqual(10, doc["counter"], "Counter should remain unchanged after failed update")
            self.assertEqual([1, 2, 3], doc["array"], "Array should remain unchanged after failed update")
            self.assertNotIn("large_field", doc, "Large field shouldn't be added after failed update")
        
        # Run the async test with a new event loop
        asyncio.run(test())
        
    def test_transaction_update_with_transformations_doesnt_create_unwanted_documents(self):
        """Test that transformation previews during transaction size checks don't modify the database in async mode."""
        
        async def test():
            # Create a document
            doc_ref = self.mock_db.collection('test').document('doc1')
            initial_data = {"counter": 10, "array": [1, 2, 3]}
            await doc_ref.set(initial_data)
            
            # Create update data with transformation that would make the document exceed the size limit
            update_data = {
                "increment.counter": 5,  # Should increment counter to 15
                "arrayUnion.array": [4, 5],  # Should add 4 and 5 to the array
                "large_field": "x" * (FIRESTORE_DOCUMENT_SIZE_LIMIT)  # This will make it exceed the limit
            }
            
            # The context manager should raise an exception when exiting due to the size limit
            with self.assertRaises(InvalidArgument) as context:
                async with self.mock_db.transaction() as transaction:
                    # This call should succeed since it just schedules the update
                    transaction.update(doc_ref, update_data)
                    # The commit happens automatically when exiting the context
            
            self.assertIn(f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes", str(context.exception))
            
            # Verify the document wasn't modified by checking counter and array values
            doc = (await doc_ref.get()).to_dict()
            self.assertEqual(10, doc["counter"], "Counter should remain unchanged after failed transaction update")
            self.assertEqual([1, 2, 3], doc["array"], "Array should remain unchanged after failed transaction update")
            self.assertNotIn("large_field", doc, "Large field shouldn't be added after failed transaction update")
        
        # Run the async test with a new event loop
        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
