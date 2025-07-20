from typing import Dict, List, Optional, Any, TypeVar, Union
import asyncio
import warnings
from datetime import datetime
from mockfirestore._helpers import Store, collection_mark_path_element, get_by_path, set_by_path, Timestamp, DELETE_FIELD
from mockfirestore.exceptions import NotFound

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

    async def set(self, document_data: Dict, merge: bool = False) -> None:
        """Set document data.

        Args:
            document_data: The document data.
            merge: If True, fields omitted will remain unchanged.
        """
        if merge:
            try:
                existing_data = get_by_path(self._data, self._path)
                self._recursive_update(existing_data, document_data)
            except KeyError:
                set_by_path(self._data, self._path, document_data)
        else:
            set_by_path(self._data, self._path, document_data)
        self._update_time = Timestamp.from_now()
        return None

    async def update(self, field_updates: Dict, option=None) -> None:
        """Update fields in the document.

        Args:
            field_updates: The fields to update and their values.
            option: If provided, restricts the update to certain field paths.

        Raises:
            NotFound: If the document doesn't exist.
        """
        try:
            existing_data = get_by_path(self._data, self._path)
        except KeyError:
            raise NotFound('No document to update: {}'.format(self._path))

        for key, val in field_updates.items():
            if key == DELETE_FIELD:
                if isinstance(val, str):
                    self._delete_field(existing_data, val.split('.'))
            elif isinstance(key, str) and key.startswith(DELETE_FIELD):
                # Handle the case when DELETE_FIELD is used as a sentinel object
                # This supports the syntax: {firestore.DELETE_FIELD: "field_name"}
                field_to_delete = val
                if isinstance(field_to_delete, str):
                    self._delete_field(existing_data, field_to_delete.split('.'))
            elif '.' in key:
                self._update_nested_field(existing_data, key, val)
            elif key.startswith('arrayUnion.'):
                field_name = key[len('arrayUnion.'):]
                if field_name in existing_data and isinstance(existing_data[field_name], list):
                    if isinstance(val, list):
                        for item in val:
                            if item not in existing_data[field_name]:
                                existing_data[field_name].append(item)
                    else:
                        if val not in existing_data[field_name]:
                            existing_data[field_name].append(val)
                else:
                    existing_data[field_name] = val if isinstance(val, list) else [val]
            elif key.startswith('arrayRemove.'):
                field_name = key[len('arrayRemove.'):]
                if field_name in existing_data and isinstance(existing_data[field_name], list):
                    if isinstance(val, list):
                        existing_data[field_name] = [
                            item for item in existing_data[field_name] if item not in val
                        ]
                    else:
                        existing_data[field_name] = [
                            item for item in existing_data[field_name] if item != val
                        ]
            elif key.startswith('increment.'):
                field_name = key[len('increment.'):]
                if field_name in existing_data and isinstance(existing_data[field_name], (int, float)):
                    existing_data[field_name] += val
                else:
                    existing_data[field_name] = val
            else:
                existing_data[key] = val

        self._update_time = Timestamp.from_now()
        return None

    async def delete(self, option=None) -> None:
        """Delete the document.

        Args:
            option: If provided, restricts the delete to certain field paths.
        """
        parent_path, doc_id = self._path[:-1], self._path[-1]
        try:
            parent_dict = get_by_path(self._data, parent_path)
            if doc_id in parent_dict:
                parent_dict.pop(doc_id)
        except KeyError:
            pass
        return None

    def _recursive_update(self, original: Dict, update: Dict) -> None:
        """Recursively update a nested dictionary."""
        for key, val in update.items():
            if isinstance(val, dict) and key in original and isinstance(original[key], dict):
                self._recursive_update(original[key], val)
            else:
                original[key] = val

    def _update_nested_field(self, data: Dict, key: str, value: Any) -> None:
        """Update a nested field."""
        parts = key.split('.')
        current = data
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value

    def _delete_field(self, data: Dict, path: List[str]) -> None:
        """Delete a field at the specified path."""
        if not path:
            return
        if len(path) == 1:
            if path[0] in data:
                del data[path[0]]
        else:
            key, rest = path[0], path[1:]
            if key in data and isinstance(data[key], dict):
                self._delete_field(data[key], rest)
