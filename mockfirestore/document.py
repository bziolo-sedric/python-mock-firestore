from copy import deepcopy
from functools import reduce
import operator
from typing import List, Dict, Any, Optional, Iterable
from mockfirestore import NotFound, InvalidArgument
from mockfirestore._helpers import (
    Timestamp, Document, Store, collection_mark_path_element, get_by_path, set_by_path, delete_by_path,
    FIRESTORE_DOCUMENT_SIZE_LIMIT, calculate_document_size
)
from mockfirestore._transformations import apply_transformations, preview_transformations
from mockfirestore.write_result import WriteResult


class DocumentSnapshot:
    def __init__(self, reference: 'DocumentReference', data: Document) -> None:
        self.reference = reference
        self._doc = deepcopy(data)

    @property
    def id(self):
        return self.reference.id

    @property
    def exists(self) -> bool:
        return self._doc != {}

    def to_dict(self) -> Document:
        return self._doc

    @property
    def create_time(self) -> Timestamp:
        timestamp = Timestamp.from_now()
        return timestamp

    @property
    def update_time(self) -> Timestamp:
        return self.create_time

    @property
    def read_time(self) -> Timestamp:
        timestamp = Timestamp.from_now()
        return timestamp

    def get(self, field_path: str) -> Any:
        if not self.exists:
            return None
        else:
            return reduce(operator.getitem, field_path.split('.'), self._doc)

    def _get_by_field_path(self, field_path: str) -> Any:
        try:
            return self.get(field_path)
        except KeyError:
            return None


class DocumentReference:
    def __init__(
        self, 
        data: Store, 
        path: List[str],
        parent: 'CollectionReference'
    ) -> None:
        self._data = data
        self._path = path
        self.parent = parent

    def __hash__(self) -> int:
        return hash(tuple(self._path))

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False

        return self._path == other._path

    @property
    def id(self):
        return self._path[-1]

    def get(
        self,
        field_paths: Optional[Iterable[str]] = None,
        transaction: Any = None,
        retry: Any = None,
        timeout: Optional[float] = None,
    ) -> DocumentSnapshot:
        return DocumentSnapshot(self, get_by_path(self._data, self._path))

    def delete(self) -> Timestamp:
        """Delete the document.
        
        Returns:
            Timestamp: The time that the delete request was received by the server.
        """
        delete_by_path(self._data, self._path)
        return Timestamp.from_now()

    def set(self, document_data: Dict, merge: bool = False) -> WriteResult:
        """Set document data.
        
        Args:
            document_data: The document data.
            merge: If True, fields omitted will remain unchanged.
            
        Returns:
            WriteResult: The write result corresponding to the committed document.
        """
        # Check document size before setting
        doc_size = calculate_document_size(document_data)
        if doc_size > FIRESTORE_DOCUMENT_SIZE_LIMIT:
            raise InvalidArgument(
                f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes. Current size: {doc_size} bytes."
            )
            
        if merge:
            try:
                return self.update(deepcopy(document_data))
            except NotFound:
                return self.set(document_data)
        else:
            set_by_path(self._data, self._path, deepcopy(document_data))

        return WriteResult()

    def update(self, data: Dict[str, Any]) -> WriteResult:
        """Update an existing document.
        
        Args:
            data: The document data to update.
            
        Returns:
            WriteResult: The write result corresponding to the updated document.
        """
        document = get_by_path(self._data, self._path)
        if document == {}:
            raise NotFound('No document to update: {}'.format(self._path))
            
        # Preview transformations to check size without modifying the original document
        updated_doc = preview_transformations(document, deepcopy(data))
        
        # Check document size after applying updates
        doc_size = calculate_document_size(updated_doc)
        if doc_size > FIRESTORE_DOCUMENT_SIZE_LIMIT:
            raise InvalidArgument(
                f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes after update. Current size: {doc_size} bytes."
            )
        
        # Apply the update to the actual document only if size check passes
        apply_transformations(document, deepcopy(data))

        return WriteResult()

    def collection(self, name: str) -> 'CollectionReference':
        from mockfirestore.collection import CollectionReference
        marked_name = collection_mark_path_element(name)

        document = get_by_path(self._data, self._path)
        new_path = self._path + [marked_name]
        if marked_name not in document:
            set_by_path(self._data, new_path, {})
        return CollectionReference(self._data, new_path, parent=self)

    def create(
        self,
        document_data: Dict[str, Any],
        retry: Optional[Any] = None,
        timeout: Optional[float] = None,
    ) -> WriteResult:
        """Create a document if it doesn't exist.
        
        Args:
            document_data: The document data.
            retry: Retry configuration used if the operation fails.
            timeout: Timeout for the operation.
            
        Returns:
            WriteResult: The write result corresponding to the committed document.
        """
        # Check document size before creating
        doc_size = calculate_document_size(document_data)
        if doc_size > FIRESTORE_DOCUMENT_SIZE_LIMIT:
            raise InvalidArgument(
                f"Document exceeds maximum size of {FIRESTORE_DOCUMENT_SIZE_LIMIT} bytes. Current size: {doc_size} bytes."
            )
        return self.set(document_data=document_data, merge=False)
