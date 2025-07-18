from typing import Any, Dict, List, Optional, Tuple, Union, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from mockfirestore.async_.aggregation import AsyncAggregationQuery
import asyncio
import operator
import re
from mockfirestore._helpers import get_by_path


class AsyncQuery:
    """Asynchronous query implementation."""

    # Field filters for different operations
    FieldFilter = Tuple[str, str, Any]
    
    # Factory class methods for common filter types
    @classmethod
    def make_field_filter(cls, field=None, op=None, value=None, filter=None):
        """Create a field filter.
        
        Args:
            field: The field to filter on.
            op: The operator to filter with.
            value: The value to compare against.
            filter: A composite filter for complex queries.
            
        Returns:
            A field filter.
        """
        if filter is not None:
            return filter
        return (field, op, value)

    # Comparison operators for filtering
    OPERATORS = {
        '<': operator.lt,
        '<=': operator.le,
        '==': operator.eq,
        '>=': operator.ge,
        '>': operator.gt,
        '!=': operator.ne,
        'array_contains': lambda array, value: value in array if isinstance(array, list) else False,
        'array_contains_any': lambda array, values: any(value in array for value in values) if isinstance(array, list) else False,
        'in': lambda value, values: value in values,
        'not-in': lambda value, values: value not in values
    }

    def __init__(self, parent, field_filters=(), orders=(), limit=None, 
                 limit_to_last=False, offset=None, start_at=None, end_at=None,
                 all_descendants=True, projection=None, recursive=False):
        self._parent = parent
        self._field_filters = field_filters
        self._orders = orders
        self._limit = limit
        self._limit_to_last = limit_to_last
        self._offset = offset
        self._start_at = start_at
        self._end_at = end_at
        self._all_descendants = all_descendants
        self._projection = projection
        self._recursive = recursive

    def _copy(self, **kwargs):
        """Create a copy of this query with the specified changes.
        
        Args:
            **kwargs: The changes to apply to the query.
            
        Returns:
            A new AsyncQuery with the changes applied.
        """
        args = dict(
            parent=self._parent,
            field_filters=self._field_filters,
            orders=self._orders,
            limit=self._limit,
            limit_to_last=self._limit_to_last,
            offset=self._offset,
            start_at=self._start_at,
            end_at=self._end_at,
            all_descendants=self._all_descendants,
            projection=self._projection,
            recursive=self._recursive
        )
        args.update(kwargs)
        return AsyncQuery(**args)

    def where(self, field=None, op=None, value=None, filter=None):
        """Create a query with a filter.
        
        Args:
            field: The field to filter on.
            op: The operator to filter with.
            value: The value to compare against.
            filter: A composite filter for complex queries.
            
        Returns:
            A new AsyncQuery with the filter applied.
        """
        new_filters = self._field_filters + (self.make_field_filter(field, op, value, filter),)
        return self._copy(field_filters=new_filters)

    def order_by(self, field_path: str, direction: Optional[str] = None):
        """Create a query with an order.
        
        Args:
            field_path: The field to order by.
            direction: The direction to order in ('ASCENDING' or 'DESCENDING').
            
        Returns:
            A new AsyncQuery with the order applied.
        """
        new_orders = self._orders + ((field_path, direction),)
        return self._copy(orders=new_orders)

    def limit(self, count: int):
        """Create a query with a limit.
        
        Args:
            count: The maximum number of documents to return.

        Returns:
            A new AsyncQuery with the limit applied.
        """
        return self._copy(limit=count)

    def limit_to_last(self, count: int):
        """Create a query with a limit, returning the last matching documents.
        
        Args:
            count: The maximum number of documents to return.
            
        Returns:
            A new AsyncQuery with the limit applied.
        """
        return self._copy(limit=count, limit_to_last=True)

    def offset(self, num_to_skip: int):
        """Create a query with an offset.
        
        Args:
            num_to_skip: The number of documents to skip.

        Returns:
            A new AsyncQuery with the offset applied.
        """
        return self._copy(offset=num_to_skip)

    def start_at(self, document_fields_or_snapshot):
        """Create a query with a start point.
        
        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.
            
        Returns:
            A new AsyncQuery with the start point applied.
        """
        return self._copy(start_at=(document_fields_or_snapshot, True))

    def start_after(self, document_fields_or_snapshot):
        """Create a query with a start point after a document.
        
        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.
            
        Returns:
            A new AsyncQuery with the start point applied.
        """
        return self._copy(start_at=(document_fields_or_snapshot, False))

    def end_at(self, document_fields_or_snapshot):
        """Create a query with an end point.
        
        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.
            
        Returns:
            A new AsyncQuery with the end point applied.
        """
        return self._copy(end_at=(document_fields_or_snapshot, True))

    def end_before(self, document_fields_or_snapshot):
        """Create a query with an end point before a document.
        
        Args:
            document_fields_or_snapshot: A dictionary of field values or a document snapshot.
            
        Returns:
            A new AsyncQuery with the end point applied.
        """
        return self._copy(end_at=(document_fields_or_snapshot, False))

    def select(self, field_paths: Iterable[str]):
        """Create a query with a projection.
        
        Args:
            field_paths: The fields to include in the result.
            
        Returns:
            A new AsyncQuery with the projection applied.
        """
        return self._copy(projection=field_paths)
        
    def count(self, alias: Optional[str] = None) -> 'AsyncAggregationQuery':
        """Adds a count over the query.
        
        Args:
            alias: Optional name of the field to store the result.
            
        Returns:
            An AsyncAggregationQuery with the count aggregation.
        """
        from mockfirestore.async_.aggregation import AsyncAggregationQuery
        return AsyncAggregationQuery(self, alias).count(alias)
        
    def avg(self, field_ref, alias: Optional[str] = None) -> 'AsyncAggregationQuery':
        """Adds an average over the query.
        
        Args:
            field_ref: The field to aggregate across.
            alias: Optional name of the field to store the result.
            
        Returns:
            An AsyncAggregationQuery with the average aggregation.
        """
        from mockfirestore.async_.aggregation import AsyncAggregationQuery
        return AsyncAggregationQuery(self, alias).avg(field_ref, alias)
        
    def sum(self, field_ref, alias: Optional[str] = None) -> 'AsyncAggregationQuery':
        """Adds a sum over the query.
        
        Args:
            field_ref: The field to aggregate across.
            alias: Optional name of the field to store the result.
            
        Returns:
            An AsyncAggregationQuery with the sum aggregation.
        """
        from mockfirestore.async_.aggregation import AsyncAggregationQuery
        return AsyncAggregationQuery(self, alias).sum(field_ref, alias)

    async def get(self, transaction=None):
        """Get all documents matching the query.
        
        Args:
            transaction: If provided, the operation will be executed within
                this transaction.
                
        Returns:
            A list of AsyncDocumentSnapshot objects.
        """
        results = []
        async for doc in self.stream(transaction=transaction):
            results.append(doc)
        return results

    async def stream(self, transaction=None):
        """Stream the documents matching the query.
        
        Args:
            transaction: If provided, the operation will be executed within
                this transaction.
                
        Returns:
            An asynchronous iterator of AsyncDocumentSnapshot objects.
        """
        from mockfirestore.async_.document import AsyncDocumentSnapshot
        
        collection_data = get_by_path(self._parent._data, self._parent._path)
        
        # Apply filters
        filtered_data = {}
        for doc_id, doc_data in collection_data.items():
            if self._passes_filters(doc_data):
                filtered_data[doc_id] = doc_data
        
        # Apply ordering
        ordered_doc_ids = self._apply_ordering(filtered_data)
        
        # Apply pagination (offset & limit)
        if self._offset:
            ordered_doc_ids = ordered_doc_ids[self._offset:]
        if self._limit:
            ordered_doc_ids = ordered_doc_ids[:self._limit]
        
        # Yield snapshots one by one
        for doc_id in ordered_doc_ids:
            doc_ref = self._parent.document(doc_id)
            doc_snapshot = await doc_ref.get()
            yield doc_snapshot

    def _passes_filters(self, doc_data):
        """Check if a document passes all filters.
        
        Args:
            doc_data: The document data.
            
        Returns:
            True if the document passes all filters, False otherwise.
        """
        for field_filter in self._field_filters:
            if isinstance(field_filter, And):
                if not all(self._passes_single_filter(doc_data, f) for f in field_filter.filters):
                    return False
            elif isinstance(field_filter, Or):
                if not any(self._passes_single_filter(doc_data, f) for f in field_filter.filters):
                    return False
            else:
                if not self._passes_single_filter(doc_data, field_filter):
                    return False
        return True

    def _passes_single_filter(self, doc_data, field_filter):
        """Check if a document passes a single filter.
        
        Args:
            doc_data: The document data.
            field_filter: The filter to check against.
            
        Returns:
            True if the document passes the filter, False otherwise.
        """
        field, op, value = field_filter
        
        if '.' in field:  # Handle nested fields
            parts = field.split('.')
            current = doc_data
            for part in parts[:-1]:
                if part not in current:
                    return False
                current = current[part]
            field = parts[-1]
            if field not in current:
                return False
            doc_value = current[field]
        else:
            if field not in doc_data:
                return False
            doc_value = doc_data[field]
            
        # Handle special cases for array operations
        if op in ('array_contains', 'array_contains_any'):
            if not isinstance(doc_value, list):
                return False
                
        # Get the operator function and apply it
        op_func = self.OPERATORS.get(op)
        if op_func is None:
            return False
            
        return op_func(doc_value, value)

    def _apply_ordering(self, filtered_data):
        """Apply ordering to filtered data.
        
        Args:
            filtered_data: The filtered document data.
            
        Returns:
            A list of document IDs in the ordered sequence.
        """
        if not self._orders:
            return sorted(filtered_data.keys())
            
        items = list(filtered_data.items())
        
        # Apply each order sequentially
        for key, direction in reversed(self._orders):
            reverse = direction == 'DESCENDING'
            
            # Handle nested fields
            if '.' in key:
                parts = key.split('.')
                
                def get_value(item):
                    doc_id, doc_data = item
                    current = doc_data
                    for part in parts:
                        if part not in current:
                            return None
                        current = current[part]
                    return current
            else:
                def get_value(item):
                    doc_id, doc_data = item
                    return doc_data.get(key)
                    
            items.sort(key=get_value, reverse=reverse)
            
        return [doc_id for doc_id, _ in items]


class Filter:
    """Base filter class for composing filters."""
    def __init__(self, filters):
        self.filters = filters


class And(Filter):
    """AND filter that combines multiple filters with AND logic."""
    pass


class Or(Filter):
    """OR filter that combines multiple filters with OR logic."""
    pass
