from typing import Iterable, Sequence, List, Optional, Union
import asyncio
from mockfirestore._helpers import collection_mark_path, collection_mark_path_element, generate_random_string, Store, get_by_path, set_by_path
from mockfirestore.async_.collection import AsyncCollectionReference, AsyncCollectionGroup
from mockfirestore.async_.document import AsyncDocumentReference, AsyncDocumentSnapshot
from mockfirestore.async_.transaction import AsyncTransaction, AsyncBatch


class AsyncMockFirestore:
    """Asynchronous mock implementation of the Firestore client."""

    def __init__(self) -> None:
        self._data = {}

    def _ensure_path(self, path):
        current_position = self

        for el in path[:-1]:
            if type(current_position) in (AsyncMockFirestore, AsyncDocumentReference):
                current_position = current_position.collection(el)
            else:
                current_position = current_position.document(el)

        return current_position

    def document(self, path: str) -> AsyncDocumentReference:
        """Get a document reference.

        Args:
            path: A slash-separated path to a document.

        Returns:
            An AsyncDocumentReference.

        Raises:
            Exception: If the path doesn't match the expected format.
        """
        path = path.split("/")

        if len(path) % 2 != 0:
            raise Exception("Cannot create document at path {}".format(path))
        
        current_position = self._ensure_path(path)
        return current_position.document(path[-1])

    def collection(self, path: str) -> AsyncCollectionReference:
        """Get a collection reference.

        Args:
            path: A slash-separated path to a collection.

        Returns:
            An AsyncCollectionReference.

        Raises:
            Exception: If the path doesn't match the expected format.
        """
        path = path.split("/")

        if len(path) % 2 != 1:
            raise Exception("Cannot create collection at path {}".format(path))
        
        path = collection_mark_path(path)
        
        name = path[-1]

        if len(path) > 1:
            current_position = self._ensure_path(path)
            return current_position.collection(name)
        else:
            if name not in self._data:
                set_by_path(self._data, [name], {})
            return AsyncCollectionReference(self._data, [name])

    async def collections(self) -> Sequence[AsyncCollectionReference]:
        """List all top-level collections.

        Returns:
            A list of AsyncCollectionReference objects.
        """
        return [
            AsyncCollectionReference(self._data, [collection_name])
            for collection_name in self._data
        ]

    def collection_group(self, collection_id: str) -> "AsyncCollectionGroup":
        """Get a reference to a collection group.

        Args:
            collection_id: Identifier for the collection group.

        Returns:
            An AsyncCollectionGroup instance.
        """
        collection_id = collection_mark_path_element(collection_id)

        return AsyncCollectionGroup(self._data, collection_id)

    def reset(self):
        """Reset the mock firestore data."""
        self._data = {}

    async def get_all(
        self,
        references: Iterable[AsyncDocumentReference],
        field_paths=None,
        transaction=None
    ) -> List[AsyncDocumentSnapshot]:
        """Get multiple documents from the database.

        Args:
            references: List of document references to retrieve.
            field_paths: If provided, only these fields will be present in
                the returned documents.
            transaction: If provided, the operation will be executed within
                this transaction.

        Returns:
            List of AsyncDocumentSnapshot objects.
        """
        docs = []
        for doc_ref in set(references):
            docs.append(await doc_ref.get())
        return docs

    def transaction(self, **kwargs) -> AsyncTransaction:
        """Create a transaction.

        Args:
            **kwargs: Transaction options.

        Returns:
            An AsyncTransaction object.
        """
        return AsyncTransaction(self, **kwargs)

    def batch(self) -> AsyncBatch:
        """Create a batch.

        Returns:
            An AsyncBatch object.
        """
        return AsyncBatch(self)
