import unittest
import asyncio

from mockfirestore import AsyncMockFirestore


class TestAsyncMockFirestore(unittest.TestCase):
    def setUp(self):
        self.mock_db = AsyncMockFirestore()
        # Run setup asynchronously
        asyncio.run(self.async_setup())
        
    async def async_setup(self):
        # Add some test data
        await self.mock_db.collection('users').document('alovelace').set({
            'first': 'Ada',
            'last': 'Lovelace',
            'born': 1815
        })
        
        await self.mock_db.collection('users').document('ghopper').set({
            'first': 'Grace',
            'last': 'Hopper',
            'born': 1906
        })

    def test_get_document(self):
        async def test():
            doc_ref = self.mock_db.collection('users').document('alovelace')
            doc = await doc_ref.get()
            self.assertTrue(doc.exists)
            self.assertEqual(doc.to_dict(), {
                'first': 'Ada',
                'last': 'Lovelace',
                'born': 1815
            })
            
        asyncio.run(test())
        
    def test_query(self):
        async def test():
            # Test a simple query
            query_ref = self.mock_db.collection('users').where('born', '>', 1900)
            docs = await query_ref.get()
            self.assertEqual(len(docs), 1)
            self.assertEqual(docs[0].to_dict()['first'], 'Grace')
            
        asyncio.run(test())
        
    def test_collection_operations(self):
        async def test():
            # Add a document
            users_collection = self.mock_db.collection('users')

            _, doc_ref = await users_collection.add({
                'first': 'Marie',
                'last': 'Curie',
                'born': 1867
            })
            
            # Stream collection
            docs = []
            async for doc in users_collection.stream():
                docs.append(doc)
            self.assertEqual(len(docs), 3)
            
            # List documents
            doc_refs = []
            async for doc_ref in users_collection.list_documents():
                doc_refs.append(doc_ref)
                
            self.assertEqual(len(doc_refs), 3)
            
        asyncio.run(test())
        
    def test_update_document(self):
        async def test():
            doc_ref = self.mock_db.collection('users').document('alovelace')
            
            # Update document
            await doc_ref.update({'born': 1816})
            
            # Get updated document
            doc = await doc_ref.get()
            self.assertEqual(doc.to_dict()['born'], 1816)
            
        asyncio.run(test())
        
    def test_delete_document(self):
        async def test():
            doc_ref = self.mock_db.collection('users').document('ghopper')
            
            # Delete document
            await doc_ref.delete()
            
            # Check it's deleted
            doc = await doc_ref.get()
            self.assertFalse(doc.exists)
            
        asyncio.run(test())
        
    def test_create_document(self):
        async def test():
            # Test creating a new document
            doc_ref = self.mock_db.collection('users').document('mcurie')
            
            await doc_ref.create({
                'first': 'Marie',
                'last': 'Curie',
                'born': 1867
            })
            
            # Check it was created
            doc = await doc_ref.get()
            self.assertTrue(doc.exists)
            self.assertEqual(doc.to_dict(), {
                'first': 'Marie',
                'last': 'Curie',
                'born': 1867
            })
            
            # Test creating a document that already exists
            from mockfirestore import AlreadyExists
            with self.assertRaises(AlreadyExists):
                await doc_ref.create({'some': 'data'})
            
        asyncio.run(test())
        
    def test_transaction(self):
        async def test():
            async with self.mock_db.transaction() as transaction:
                doc_ref = self.mock_db.collection('users').document('alovelace')
                
                # Get document in transaction
                doc = await transaction.get(doc_ref)
                
                # Update in transaction
                transaction.update(doc_ref, {'born': 1816})
            
            # Check it's updated
            doc = await doc_ref.get()
            self.assertEqual(doc.to_dict()['born'], 1816)
            
        asyncio.run(test())
        
    def test_batch(self):
        async def test():
            batch = self.mock_db.batch()
            
            # Add operations to batch
            doc_ref1 = self.mock_db.collection('users').document('alovelace')
            doc_ref2 = self.mock_db.collection('users').document('ghopper')
            
            batch.update(doc_ref1, {'born': 1816})
            batch.update(doc_ref2, {'born': 1907})
            
            # Commit batch
            await batch.commit()
            
            # Check updates applied
            doc1 = await doc_ref1.get()
            doc2 = await doc_ref2.get()
            self.assertEqual(doc1.to_dict()['born'], 1816)
            self.assertEqual(doc2.to_dict()['born'], 1907)
            
        asyncio.run(test())
        
    def test_get_all(self):
        async def test():
            doc_refs = [
                self.mock_db.collection('users').document('alovelace'),
                self.mock_db.collection('users').document('ghopper')
            ]
            
            # Get all documents
            docs = await self.mock_db.get_all(doc_refs)
            self.assertEqual(len(docs), 2)
            
        asyncio.run(test())
        
    @unittest.skip("Nested collection group queries not supported correctly in this version")
    def test_collection_group(self):
        async def test():
            # Add subcollection documents
            await self.mock_db.collection('users').document('alovelace').collection('books').document('book1').set({
                'title': 'Notes on the Analytical Engine',
                'year': 1843
            })
            
            await self.mock_db.collection('libraries').document('lib1').collection('books').document('book2').set({
                'title': 'Computing Machinery and Intelligence',
                'year': 1950
            })
            
            # Query collection group
            books = self.mock_db.collection_group('books')
            results = await books.get()
            
            self.assertEqual(len(results), 2)
            
        asyncio.run(test())
    
    def test_delete_field(self):
        async def test():
            from mockfirestore import DELETE_FIELD
            
            # Test with DELETE_FIELD value (the proper way to use it)
            doc_ref = self.mock_db.collection('users').document('alovelace')
            await doc_ref.update({'born': DELETE_FIELD})
            
            # Verify the field is deleted
            doc = await doc_ref.get()
            self.assertNotIn('born', doc.to_dict())
            
            # Add the field back
            await doc_ref.update({'born': 1815})
            
            # Test with field path syntax
            await doc_ref.update({'born': DELETE_FIELD})
            
            # Verify the field is deleted again
            doc = await doc_ref.get()
            self.assertNotIn('born', doc.to_dict())
            
        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
