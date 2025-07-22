import operator
import random
import string
from datetime import datetime as dt
from functools import reduce
from typing import (Callable, Dict, Any, Tuple, TypeVar, Sequence, Iterator)

T = TypeVar('T')
KeyValuePair = Tuple[str, Dict[str, Any]]
Document = Dict[str, Any]
Collection = Dict[str, Document]
Store = Dict[str, Collection]

# Firestore document size limit (1MB)
FIRESTORE_DOCUMENT_SIZE_LIMIT = 1_048_576  # 1MB in bytes

COLLECTION_FIELD_NAME_FORMAT = "__{name}_collection__"
PATH_ELEMENT_SEPARATOR = "<.>"

# NOTE 1: To allow collections as part of the dictionary, which is functionally incorrect, as per Firestore functionality,
# NOTE 2: we need to rename the collection path elements to avoid conflicts with the dictionary keys.

def is_path_element_collection_marked(path: str) -> bool:
    """Check if the path is marked."""
    return path.startswith('__') and path.endswith('_collection__')


def collection_mark_path_element(path_element: str) -> str:
    """Mark a path element to avoid conflicts with dictionary keys."""
    if not is_path_element_collection_marked(path_element):
        return COLLECTION_FIELD_NAME_FORMAT.format(name=path_element)
    return path_element


def collection_mark_path(path: Sequence[str]) -> Sequence[str]:
    """Mark path elements to avoid conflicts with dictionary keys."""
    return [
        collection_mark_path_element(
            path_element) if i % 2 == 0 else path_element
        for i, path_element in enumerate(path)
    ]


def traverse_dict(dictionary: Dict[str, Any], key_value_operator: Callable[[str, str, Any], None], path: str = ""):
    for key, value in dictionary.items():
        current_path = f"{path}{PATH_ELEMENT_SEPARATOR}{key}" if path else key
        key_value_operator(key, current_path, value)
        if isinstance(value, dict):
            traverse_dict(value, key_value_operator, current_path)


def get_by_path(data: Dict[str, T], path: Sequence[str], create_nested: bool = False) -> T:
    """Access a nested object in root by item sequence."""

    def get_or_create(_data, _path):
        if _path not in _data:
            _data[_path] = {}
        return _data[_path]

    if create_nested:
        return reduce(get_or_create, path, data)
    else:
        return reduce(operator.getitem, path, data)


def _normalize_nested(value):
    """Iterate a nested object, and handle initial transformations e.g Increment"""
    if isinstance(value, dict):
        return {k: _normalize_nested(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_normalize_nested(v) for v in value]
    elif hasattr(value, 'value'):
        return value.value
    else:
        return value


def set_by_path(data: Dict[str, T], path: Sequence[str], value: T, create_nested: bool = True):
    """Set a value in a nested object in root by item sequence."""
    value = _normalize_nested(value)
    get_by_path(data, path[:-1], create_nested=True)[path[-1]] = value


def delete_by_path(data: Dict[str, T], path: Sequence[str]):
    """Delete a value in a nested object in root by item sequence."""
    del get_by_path(data, path[:-1])[path[-1]]


def generate_random_string():
    letters = string.ascii_lowercase
    digits = string.digits
    return ''.join(random.choice(letters + digits) for _ in range(20))

def calculate_document_size(document_data: Dict[str, Any]) -> int:
    """
    Calculate the approximate size of a document in bytes.

    This provides a somewhat accurate estimate of Firestore's document size calculation
    by accounting for the size of individual fields and their values.
    
    Args:
        document_data: The document data to calculate the size for.
        
    Returns:
        The approximate size of the document in bytes.
    """
    def to_serializable(val):
        if hasattr(val, "__dataclass_fields__"):  # Check if it's a dataclass
            try:
                from dataclasses import asdict
                return asdict(val)
            except (ImportError, TypeError):
                return vars(val)
        elif hasattr(val, "__dict__"):
            return vars(val)
        else:
            return val

    def _estimate(value, skip_collections=True):
        value = to_serializable(value)

        if value is None:
            return 1  # null
        elif isinstance(value, bool):
            return 1  # true/false
        elif isinstance(value, (int, float)):
            return 8  # Firestore stores numbers as 64-bit
        elif isinstance(value, str):
            return len(value.encode('utf-8'))
        elif isinstance(value, list):
            return sum(_estimate(v, skip_collections) + 1 for v in value)
        elif isinstance(value, dict):
            size = 0
            for k, v in value.items():
                # Skip collection markers (internal implementation detail)
                if skip_collections and is_path_element_collection_marked(k):
                    continue
                    
                key_size = len(str(k).encode('utf-8')) + 1  # +1 for field overhead
                val_size = _estimate(v, skip_collections)
                size += key_size + val_size
            return size
        else:
            # For unsupported types, convert to string as a fallback
            try:
                return len(str(value).encode('utf-8'))
            except Exception:
                return 8  # Default size if we can't determine it
    
    # Return the size estimate in bytes
    return _estimate(document_data, skip_collections=True)


class Timestamp:
    """
    Imitates some properties of `google.protobuf.timestamp_pb2.Timestamp`
    """

    def __init__(self, timestamp: float):
        self._timestamp = timestamp

    @classmethod
    def from_now(cls):
        timestamp = dt.now().timestamp()
        return cls(timestamp)

    @property
    def seconds(self):
        return str(self._timestamp).split('.')[0]

    @property
    def nanos(self):
        return str(self._timestamp).split('.')[1]


def get_document_iterator(document: Dict[str, Any], prefix: str = '') -> Iterator[Tuple[str, Any]]:
    """
    :returns: (dot-delimited path, value,)
    """
    for key, value in document.items():
        if isinstance(value, dict):
            for item in get_document_iterator(value, prefix=key):
                yield item

        if not prefix:
            yield key, value
        else:
            yield '{}.{}'.format(prefix, key), value
