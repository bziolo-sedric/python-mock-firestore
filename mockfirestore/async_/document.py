from typing import Dict, List, Optional, Any, TypeVar, Union, TYPE_CHECKING
import copy
from datetime import datetime
from mockfirestore._helpers import (
    Store, collection_mark_path_element, get_by_path, set_by_path, Timestamp,
    FIRESTORE_DOCUMENT_SIZE_LIMIT, calculate_document_size
)
from mockfirestore import NotFound as NotFoundError, InvalidArgument
from mockfirestore._transformations import preview_transformations, apply_transformations
from mockfirestore.write_result import WriteResult

# Forward references for type checking
if TYPE_CHECKING:
    from mockfirestore.async_.collection import AsyncCollectionReference


T = TypeVar('T')

class AsyncDocumentSnapshot:
    """Asynchronous document snapshot."""

    def __init__(self, reference, data):
        self._reference = reference
        self._data = data
        self._read_time = Timestamp.from_now()
        self._create_time = self._read_time
        self._update_time = self._read_time

    def __eq__(self, other):
        if isinstance(other, AsyncDocumentSnapshot):
            return (
                self._reference == other._reference
                and self._data == other._data
            )
        return NotImplemented

    @property
    def exists(self) -> bool:
        """bool: True if the document exists, False otherwise."""
        return self._data is not None
    
    @property
    def id(self) -> str:
        """str: The document identifier."""
        return self._reference.id
    
    @property
    def reference(self) -> 'AsyncDocumentReference':
        """AsyncDocumentReference: The document reference that produced this snapshot."""
        return self._reference

    @property
    def create_time(self) -> datetime:
        """datetime: The creation time of the document."""
        return self._create_time

    @property
    def update_time(self) -> datetime:
        """datetime: The last update time of the document."""
        return self._update_time

    @property
    def read_time(self) -> datetime:
        """datetime: The time this snapshot was read."""
        return self._read_time

    def get(self, field_path: str, default: Optional[T] = None) -> Union[Any, T]:
        """Get a field value from the document.

        Args:
            field_path: A dot-separated string of field names.
            default: Value to return if the field doesn't exist.

        Returns:
            The value at the specified field path or the default value.
        """
        if not self.exists:
            return default

        if not field_path:
            return default

        parts = field_path.split('.')
        value = self._data.copy()
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default
        return value

    def to_dict(self) -> Dict[str, Any]:
        """Convert the document to a dictionary.

        Returns:
            The document as a dictionary or None if the document doesn't exist.
        """
        return self._data.copy() if self._data else None


class AsyncDocumentReference:
    """Asynchronous document reference."""

    def __init__(self, data: Store, path: List[str], parent=None) -> None:
        self._data = data
        self._path = path
        self.parent = parent
        self._update_time = Timestamp.from_now()
        self._read_time = self._update_time

    def __eq__(self, other):
        if isinstance(other, AsyncDocumentReference):
            return self._path == other._path
        return NotImplemented
        
    def __hash__(self) -> int:
        """Make the document reference hashable.
        
        Returns:
            A hash of the document path.
        """
        return hash(tuple(self._path))

    @property
    def id(self) -> str:
        """str: The document identifier."""
        return self._path[-1]
    
    @property
    def path(self) -> str:
        """str: The document path."""
        return '/'.join(self._path)

    @property
    def update_time(self) -> datetime:
        """datetime: The last update time of the document."""
        return self._update_time

    @property
    def read_time(self) -> datetime:
        """datetime: The last read time of the document."""
        return self._read_time

    def collection(self, collection_id: str) -> 'AsyncCollectionReference':
        """Get a collection reference.

        Args:
            collection_id: The collection identifier.

        Returns:
            An AsyncCollectionReference.
        """
        from mockfirestore.async_.collection import AsyncCollectionReference
        marked_name = collection_mark_path_element(collection_id)

        document = get_by_path(self._data, self._path)
        new_path = self._path + [marked_name]
        if marked_name not in document:
            set_by_path(self._data, new_path, {})
        return AsyncCollectionReference(self._data, new_path, parent=self)

    async def get(self, field_paths=None, transaction=None, retry=None, timeout=None) -> AsyncDocumentSnapshot:
        """Get a document snapshot.

        Args:
            field_paths: If provided, only these fields will be present in
                the returned document.
            transaction: If provided, the operation will be executed within
                this transaction.

        Returns:
            An AsyncDocumentSnapshot.

        Raises:
            NotFound: If the document doesn't exist.
        """
        try:
            document_data = get_by_path(self._data, self._path)
            return AsyncDocumentSnapshot(self, document_data)
        except KeyError:
            return AsyncDocumentSnapshot(self, None)

    async def create(self, document_data: Dict, retry=None, timeout=None) -> WriteResult:
        """Create a document if it doesn't exist.

        Args:
            document_data: The document data.
            retry: Retry configuration used if the operation fails.
            timeout: Timeout for the operation.

        Raises:
            AlreadyExists: If the document already exists.
            InvalidArgument: If the document exceeds the 1MB size limit.
            
        Returns:
            WriteResult: The write result corresponding to the committed document.
        """
        # Check document size before creating
        doc_size = calculate_document_size(document_data)
        if doc_size > FIRESTORE_DOCUMENT_SIZE_LIMIT:
            raise InvalidArgument(
                f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes. Current size: {doc_size} bytes."
            )
            
        # Check if document already exists
        try:
            existing_doc = get_by_path(self._data, self._path)
            if existing_doc:  # Document exists and has data
                from mockfirestore import AlreadyExists
                raise AlreadyExists(f"Document already exists: {self._path}")
        except KeyError:
            # KeyError means document path doesn't exist yet - this is good
            pass
            
        # Document doesn't exist or is empty, so we can create it
        set_by_path(self._data, self._path, document_data)
        self._update_time = Timestamp.from_now()

        return WriteResult()

    async def set(self, document_data: Dict, merge: bool = False) -> WriteResult:
        """Set document data.

        Args:
            document_data: The document data.
            merge: If True, fields omitted will remain unchanged.
            
        Raises:
            InvalidArgument: If the document exceeds the 1MB size limit.
            
        Returns:
            WriteResult: The write result corresponding to the committed document.
        """
        # Check document size before setting
        doc_size = calculate_document_size(document_data)
        if doc_size > FIRESTORE_DOCUMENT_SIZE_LIMIT:
            raise InvalidArgument(
                f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes. Current size: {doc_size} bytes."
            )

        document_data_copy = copy.deepcopy(document_data)
            
        if merge:
            try:
                existing_data = get_by_path(self._data, self._path)
                
                # Make a copy for size checking
                test_data = copy.deepcopy(existing_data)

                # Use preview_transformations to simulate the merge without modifying data
                preview_data = preview_transformations(test_data, document_data_copy)

                # Check the size after merge
                merged_size = calculate_document_size(preview_data)
                if merged_size > FIRESTORE_DOCUMENT_SIZE_LIMIT:
                    raise InvalidArgument(
                        f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes after merge. Current size: {merged_size} bytes."
                    )
                
                # Apply the update to the actual document using apply_transformations
                apply_transformations(existing_data, document_data_copy)
            except KeyError:
                set_by_path(self._data, self._path, document_data_copy)
        else:
            set_by_path(self._data, self._path, document_data_copy)
        self._update_time = Timestamp.from_now()
        
        return WriteResult()

    async def update(self, data: Dict[str, Any]) -> WriteResult:
        """Update document data.
        
        Args:
            data: The document data to update.
            
        Raises:
            NotFound: If the document does not exist.
            InvalidArgument: If the document exceeds the 1MB size limit.
            
        Returns:
            WriteResult: The write result corresponding to the updated document.
        """
        # Get document snapshot using await since get() is an async method
        doc_snapshot = await self.get()
        if not doc_snapshot.exists:
            raise NotFoundError()

        document = get_by_path(self._data, self._path)

        updates = copy.deepcopy(data)

        # Preview transformations to check size without modifying the original document
        updated_doc = preview_transformations(document, updates)

        # Check document size after applying updates
        doc_size = calculate_document_size(updated_doc)
        if doc_size > FIRESTORE_DOCUMENT_SIZE_LIMIT:
            raise InvalidArgument(
                f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes after update. Current size: {doc_size} bytes."
            )
        
        # Apply the update to the actual document only if size check passes
        apply_transformations(document, updates)
        
        # Update the update time
        self._update_time = Timestamp.from_now()

        return WriteResult()

    async def delete(self, option=None) -> Timestamp:
        """Delete the document.

        Args:
            option: If provided, restricts the delete to certain field paths.
            
        Returns:
            Timestamp: The time that the delete request was received by the server.
        """
        parent_path, doc_id = self._path[:-1], self._path[-1]
        try:
            parent_dict = get_by_path(self._data, parent_path)
            if doc_id in parent_dict:
                parent_dict.pop(doc_id)
        except KeyError:
            pass
        
        # According to the Firestore API, delete returns a timestamp
        delete_time = Timestamp.from_now()
        return delete_time
