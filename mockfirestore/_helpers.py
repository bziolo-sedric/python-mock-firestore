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

# NOTE 1: To allow collections as part of the dictionary, which is functionally incorrect, as per Firestore functionality,
# NOTE 2: we need to rename the collection path elements to avoid conflicts with the dictionary keys.

def is_path_element_collection_marked(path: str) -> bool:
    """Check if the path is marked."""
    return path.startswith('__') and path.endswith('_collection__')


def collection_mark_path_element(path_element: str) -> str:
    """Mark a path element to avoid conflicts with dictionary keys."""
    if not is_path_element_collection_marked(path_element):
        return f'__{path_element}_collection__'
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
        current_path = f"{path}.{key}" if path else key
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
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(20))


# Sentinel value for field deletion
DELETE_FIELD = "__delete_field__"


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
