# Asynchronous Mock Firestore API

This module provides asynchronous versions of the Mock Firestore API, designed to mirror the async API provided by the official Google Cloud Firestore Python client.

## Usage

```python
from mockfirestore import AsyncMockFirestore

async def example_usage():
    # Create an async mock firestore instance
    mock_db = AsyncMockFirestore()
    
    # Add a document
    _, doc_ref = await mock_db.collection('users').add({
        'first': 'Ada',
        'last': 'Lovelace'
    })
    
    # Get a document
    doc_snapshot = await doc_ref.get()
    data = doc_snapshot.to_dict()
    
    # Query for documents
    users = await mock_db.collection('users').where('first', '==', 'Ada').get()
    
    # Use with an async transaction
    async with mock_db.transaction() as transaction:
        user_ref = mock_db.collection('users').document('alovelace')
        await transaction.set(user_ref, {'first': 'Augusta Ada'})
    
    # Use an async batch
    batch = mock_db.batch()
    batch.set(mock_db.collection('users').document('alovelace'), {'born': 1815})
    await batch.commit()

# Run with asyncio
import asyncio
asyncio.run(example_usage())
```

## Supported Operations

The async API supports all the same operations as the synchronous API, but with async/await syntax. This includes:

- Collections and documents
- Queries with filters, ordering, and pagination
- Transactions and batches
- All document operations (get, set, update, delete)

## Implementation Notes

- All methods that would return values in the synchronous API return awaitables in the async API
- Context managers for transactions use `async with` syntax
- The internal implementation mimics the behavior of the synchronous version but allows for asynchronous execution
