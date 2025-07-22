from typing import Dict, Any, List
from copy import deepcopy

from mockfirestore._helpers import get_document_iterator, get_by_path, set_by_path, delete_by_path, FIRESTORE_DOCUMENT_SIZE_LIMIT

from mockfirestore import DELETE_FIELD, Increment, ArrayUnion, ArrayRemove
# Document size constant
MAX_DOCUMENT_SIZE = FIRESTORE_DOCUMENT_SIZE_LIMIT
MAX_DOCUMENT_SIZE_HUMAN_READABLE = "1MB"


def preview_transformations(document: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates transformations without modifying the original document.
    Returns a new document showing what the result would be after applying transformations.
    
    Args:
        document: The original document to transform
        data: The update data with transformations
        
    Returns:
        A new document representing the result after applying transformations
    """
    # Create deep copies to avoid modifying originals
    document_copy = deepcopy(document)
    data_copy = deepcopy(data)
    
    # Apply transformations to the copies
    apply_transformations(document_copy, data_copy)
    
    # Return the transformed document copy
    return document_copy


def apply_transformations(document: Dict[str, Any], data: Dict[str, Any]):
    """Handles special fields like INCREMENT."""
    increments = {}
    arr_unions = {}
    arr_deletes = {}
    deletes = []

    # Special handling for DELETE_FIELD
    for key, value in list(data.items()):
        # Check if it's a special DELETE_FIELD sentinel used as a key
        if (hasattr(key, '__class__') and 
            ((key.__class__.__name__ == 'Sentinel' and hasattr(key, 'description') and 
              key.description == "Value used to delete a field in a document.") or 
             key == DELETE_FIELD)):
            # Handle DELETE_FIELD as key
            if isinstance(value, str):
                deletes.append(value)
                del data[key]
            continue
            
    # Process regular transformations
    for key, value in list(get_document_iterator(data)):
        # Check if it's from the Firestore library
        if hasattr(value, '__class__'):
            # Handle real Firestore objects
            if hasattr(value.__class__, '__module__') and value.__class__.__module__.startswith('google.cloud.firestore'):
                transformer = value.__class__.__name__
                if transformer == 'Increment':
                    increments[key] = value.value
                elif transformer == 'ArrayUnion':
                    arr_unions[key] = value.values
                elif transformer == 'ArrayRemove':
                    arr_deletes[key] = value.values
                    del data[key]
                elif transformer == 'Sentinel':
                    if value.description == "Value used to delete a field in a document.":
                        deletes.append(key)
                        del data[key]

        # All other transformations can be applied as needed.
        # See #29 for tracking.

    def _update_data(new_values: dict, default: Any):
        for key, value in new_values.items():
            path = key.split('.')

            try:
                item = get_by_path(document, path)
            except (TypeError, KeyError):
                item = default

            set_by_path(data, path, item + value, create_nested=True)

    _update_data(increments, 0)
    _update_data(arr_unions, [])

    _apply_updates(document, data)
    _apply_deletes(document, deletes)
    _apply_arr_deletes(document, arr_deletes)


def _apply_updates(document: Dict[str, Any], data: Dict[str, Any]):
    for key, value in data.items():
        # Skip Sentinel objects as they're handled separately
        if hasattr(key, '__class__') and key.__class__.__name__ == 'Sentinel':
            continue
        path = key.split(".")
        set_by_path(document, path, value, create_nested=True)


def _apply_deletes(document: Dict[str, Any], data: List[str]):
    for key in data:
        path = key.split(".")
        delete_by_path(document, path)


def _apply_arr_deletes(document: Dict[str, Any], data: Dict[str, Any]):
    for key, values_to_delete in data.items():
        path = key.split(".")
        try:
            value = get_by_path(document, path)
        except KeyError:
            continue
        for value_to_delete in values_to_delete:
            try:
                value.remove(value_to_delete)
            except ValueError:
                pass
        set_by_path(document, path, value)